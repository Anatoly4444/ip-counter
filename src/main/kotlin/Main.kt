import kotlinx.coroutines.*
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.measureTime

const val file = "C:\\Users\\lifte\\Downloads\\ip_addresses\\ip_addresses"
val errorCounter = AtomicInteger(0)
val dispatcher = Executors.newFixedThreadPool(2).asCoroutineDispatcher()

fun main() {
     println("${LocalDateTime.now()} started")

     var firstOctets: ConcurrentHashMap<String, Int>
     val time = measureTime {
          firstOctets = getFirstOctets()
     }
     println("First octets found in ${time.inWholeMinutes} minutes. size ${firstOctets.size}")

     runBlocking {
          val measureTime = measureTime {
               val count = firstOctets.map {  countAsync(it) }.awaitAll().sum()
               println("Amount of unique addresses is $count")
               println("Errors occurred $errorCounter ")
          }
          println("${LocalDateTime.now()} Found in ${measureTime.inWholeMinutes} minutes")
     }
}

private fun CoroutineScope.countAsync(item: Map.Entry<String, Int>): Deferred<Int> =
     async(dispatcher) {
          try {
               return@async countAddresses(item.key)
          } catch (e: Exception) {
               println("Exception occurred $e")
               return@async retry { countAddresses(item.key) }
          }
     }

private fun countAddresses( octet: String): Int {
     println("Octet $octet processing started")
     var value: Int
     val measureTime = measureTime {
          val hashSet = HashSet<String>(4_000_000)
          val stream = Files.lines(Path.of(file))
          stream.use {
               it.sequential().forEach { x ->
                    if (x.startsWith(octet)) {
                         hashSet.add(x)
                    }
               }
          }
          value = hashSet.count()
     }
     println("Count of octet $octet is $value. Spent $measureTime minutes")
     return value
}

private fun getFirstOctets(): ConcurrentHashMap<String, Int> {
     val stream = Files.lines(Path.of(file))
     val firstOctets = ConcurrentHashMap<String, Int>()
     stream.use {
          it.parallel().forEach { x ->
               val firstOctetOfIp = x.split(".").get(0)
               firstOctets.put("$firstOctetOfIp.", 0)
          }
     }
     return firstOctets
}

fun retry(
     times: Int = 3,
     x: () -> Int
): Int {
     repeat(times) {
          try {
               return x()
          } catch (e: Exception) {
               println("Exception occurred: $e")
          }
     }
     errorCounter.incrementAndGet()
     return 0
}