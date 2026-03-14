import asyncio
import aiohttp
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
BASE_URL = "http://ecomarket.local/api/v1"
TOKEN = "token_de_prueba_123456789012345"
INTERVALO_BASE = 5
INTERVALO_MAX = 60
TIMEOUT = 10

# ─── CONFIGURACIÓN DE LOGGING ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)

# ─── INTERFAZ OBSERVADOR ──────────────────────────────────────────────────────
class Observador(ABC):
    """Interfaz abstracta que todos los observadores deben implementar garantizando un contrato estricto."""
    @abstractmethod
    async def actualizar(self, inventario: Dict[str, Any]) -> None:
        pass

# ─── OBSERVADORES CONCRETOS ───────────────────────────────────────────────────
class ModuloCompras(Observador):
    def __init__(self):
        self.log = logging.getLogger("ModuloCompras")

    async def actualizar(self, inventario: Dict[str, Any]) -> None:
        productos = inventario.get("productos", [])
        for p in productos:
            if p.get("status") == "BAJO_MINIMO":
                self.log.warning(f"Producto crítico a reabastecer: {p.get('nombre')} (ID: {p.get('id')}). Stock: {p.get('stock')}")

class ModuloAlertas(Observador):
    def __init__(self):
        self.log = logging.getLogger("ModuloAlertas")
    
    async def actualizar(self, inventario: Dict[str, Any]) -> None:
        productos = inventario.get("productos", [])
        
        # Enfoque Avanzado: Emitir alertas concurrentemente, no secuencial.
        tareas_alertas = []
        for p in productos:
            if p.get("status") == "BAJO_MINIMO":
                tareas_alertas.append(self._enviar_alerta_api(p))
                
        if tareas_alertas:
            # gather(return_exceptions=True) protege el flujo evitando que si falla una alerta, fallen las demás
            resultados = await asyncio.gather(*tareas_alertas, return_exceptions=True)
            for res in resultados:
                if isinstance(res, Exception):
                    self.log.error(f"Fallo durante ejecución paralela de POST de alerta: {res}")

    async def _enviar_alerta_api(self, producto: Dict[str, Any]) -> None:
        url = f"{BASE_URL}/alertas"
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        }
        payload = {
            "producto_id": producto.get("id"),
            "stock_actual": producto.get("stock"),
            "stock_minimo": producto.get("stock_minimo"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            # Se usa una sesión temporal porque notificar es esporádico comparado al polling principal.
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload, timeout=TIMEOUT) as response:
                    status = response.status
                    if status == 201:
                        self.log.info(f"Alerta central registrada para producto: {producto.get('id')}")
                    elif status == 422:
                        self.log.error(f"Rechazo (422 Unprocessable Entity) alertando {producto.get('id')}. Payload rechazado, no hay reintento.")
                    else:
                        self.log.warning(f"Respuesta no esperada del servidor ({status}) para alerta de {producto.get('id')}")
        except asyncio.TimeoutError:
            self.log.error(f"Timeout (>{TIMEOUT}s) al enviar alerta de {producto.get('id')}")
        except aiohttp.ClientError as err_red:
            self.log.error(f"Error a nivel de red enviando alerta {producto.get('id')}: {err_red}")

# ─── OBSERVABLE (EL MONITOR) ──────────────────────────────────────────────────
class MonitorInventario:
    def __init__(self):
        self._observadores: List[Observador] = []
        self._ultimo_etag: Optional[str] = None
        self._ejecutando: bool = False
        self._intervalo: int = INTERVALO_BASE
        
        self.log = logging.getLogger("MonitorInventario")
        # Enfoque Avanzado: Mantenemos un "pool" persistente de conexiones subyacente de la sesión
        self._sesion_poll: Optional[aiohttp.ClientSession] = None

    def suscribir(self, observador: Observador) -> None:
        if observador not in self._observadores:
            self._observadores.append(observador)
            self.log.debug(f"Nuevo observador registrado: {observador.__class__.__name__}")

    def desuscribir(self, observador: Observador) -> None:
        if observador in self._observadores:
            self._observadores.remove(observador)
            self.log.debug(f"Observador removido: {observador.__class__.__name__}")

    async def _notificar(self, inventario: Dict[str, Any]) -> None:
        """Invoca a cada observador protegiendo el ciclo principal en caso de fallo algoritmico en alguno"""
        for obs in self._observadores:
            try:
                await obs.actualizar(inventario)
            except Exception as e:
                self.log.error(f"El observador {obs.__class__.__name__} generó una excepción: {e}")
                # El ciclo continúa protegiendo a los otros observadores (INVARIANTE)

    async def _consultar_inventario(self) -> Optional[Dict[str, Any]]:
        url = f"{BASE_URL}/inventario"
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/json"
        }
        
        if self._ultimo_etag:
            headers["If-None-Match"] = self._ultimo_etag

        try:
            if not self._sesion_poll or self._sesion_poll.closed:
                self._sesion_poll = aiohttp.ClientSession()

            async with self._sesion_poll.get(url, headers=headers, timeout=TIMEOUT) as response:
                status = response.status
                
                if status == 200:
                    datos = await response.json()
                    
                    # Validacion de datos minimos robusta
                    if not datos or "productos" not in datos or datos.get("productos") is None:
                        self.log.warning("Servidor regresó 200 OK pero la estructura del JSON es inválida.")
                        return None
                    
                    self._ultimo_etag = response.headers.get("ETag")
                    return datos

                elif status == 304:
                    self.log.debug("Estado HTTP 304 Not Modified. No se propaga actualización.")
                    return None

                elif 400 <= status < 500:
                    self.log.error(f"Falla del lado del cliente {status} (e ej. 401 Unauthorized / 400). No es posible arreglar esto reintentando en loop.")
                    return None
                    
                elif status >= 500:
                    self.log.warning(f"Error {status} del servidor (e.g. 503 Service Unavailable). Entrando en modo Backoff adaptativo.")
                    self._ampliar_backoff()
                    return None

        except asyncio.TimeoutError:
            self.log.warning(f"Timeout al intentar el polling. El servidor no respondio en {TIMEOUT}s. Se mantiene el ciclo.")
        except aiohttp.ClientError as x_red:
            self.log.warning(f"Caida de red durante el polling: {x_red}. Se mantiene el ciclo.")
        except Exception as err_fatal:
            self.log.error(f"Falla crítica desconocida durante la petición: {err_fatal}")
            
        return None

    def _ampliar_backoff(self) -> None:
        """Duplica el intervalo del ciclo hasta llegar al tope máximo marcado"""
        if self._intervalo < INTERVALO_MAX:
            self._intervalo = min(self._intervalo * 2, INTERVALO_MAX)
            self.log.warning(f"Aplicando backoff algorítmico, nuevo intervalo espera: {self._intervalo}s")

    def _restaurar_backoff(self) -> None:
        """Reestablece el ciclo al ritmo base original"""
        if self._intervalo != INTERVALO_BASE:
            self._intervalo = INTERVALO_BASE
            self.log.info(f"Backoff reseteado a tiempo base: {self._intervalo}s")

    async def iniciar(self) -> None:
        """Heartbeat base encargado de orquestar recolección de eventos y su despacho asíncrono"""
        self._ejecutando = True
        self.log.info("Despertando Orquestador (MonitorInventario)...")
        self._sesion_poll = aiohttp.ClientSession()
        
        try:
            while self._ejecutando:
                nuevo_estado = await self._consultar_inventario()
                
                if nuevo_estado is not None:
                    # Significa 200 OK limpio
                    self._restaurar_backoff()
                    await self._notificar(nuevo_estado)
                
                # Sleep de forma limpia a Asyncio (No bloquea a otras tareas usando await asyncio)
                await asyncio.sleep(self._intervalo)
        finally:
            if self._sesion_poll and not self._sesion_poll.closed:
                await self._sesion_poll.close()
            self.log.info("Motor interno del orquestador apagado de forma segura.")

    def detener(self) -> None:
        self.log.info("Señal de corte recibida, cerrando switches...")
        self._ejecutando = False

# ─── SECCIÓN EJECUCIÓN (MAIN) ─────────────────────────────────────────────────
async def main():
    monitor = MonitorInventario()
    monitor.suscribir(ModuloCompras())
    monitor.suscribir(ModuloAlertas())
    
    tarea_motor = asyncio.create_task(monitor.iniciar())
    
    # Se simula trabajar por 12 segundos...
    await asyncio.sleep(12)
    
    # Detenemos con cierre estricto graceful shutdown
    monitor.detener()
    await tarea_motor

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown via Terminal.")
