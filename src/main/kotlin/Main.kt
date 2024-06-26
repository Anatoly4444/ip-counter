import kotlinx.coroutines.*
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.exitProcess
import kotlin.time.measureTime

const val file = "C:\\Users\\lifte\\Downloads\\ip_addresses\\ip_addresses"
val errorCounter = AtomicInteger(0)
val dispatcher = Executors.newFixedThreadPool(12).asCoroutineDispatcher()

fun main() {
     println("${LocalDateTime.now()} started")
     var firstOctets: Set<String>
     val time = measureTime {
          firstOctets = getFirstOctets()
     }
     println("First octets found in ${time.inWholeMinutes}. size ${firstOctets.size}")

     val measureTime = measureTime {
          val count = runBlocking { firstOctets.map { countAsync(it) }.awaitAll().sum() }
          println("Amount of unique addresses is $count")
          println("Errors occurred $errorCounter ")
     }
     println("${LocalDateTime.now()} Found in ${measureTime.inWholeMinutes}")
     exitProcess(1)
}

private fun CoroutineScope.countAsync(item: String): Deferred<Int> =
     async(dispatcher) {
          try {
               return@async countAddresses(item)
          } catch (e: Exception) {
               println("Exception occurred $e")
               return@async retry { countAddresses(item) }
          }
     }

private fun countAddresses( octet: String): Int {
     println("Octet $octet processing started")
     var value: Int
     val measureTime = measureTime {
          val hashSet = HashSet<String>(4_000_000, 0.25f)
          val stream = Files.lines(Path.of(file))
          stream.forEach { x ->
               if (x.startsWith(octet)) {
                    hashSet.add(x)
               }
          }
          stream.close()
          value = hashSet.count()

     }
     println("Octet $octet has $value ip-addresses. Spent $measureTime")
     return value
}

private fun getFirstOctets(): Set<String> {
     val stream = Files.lines(Path.of(file))
     val firstOctets = ConcurrentHashMap.newKeySet<String>()
     stream.parallel().forEach { x ->
          val firstOctetOfIp = x.split(".").get(0)
          firstOctets.add("$firstOctetOfIp.")
     }
     stream.close()
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