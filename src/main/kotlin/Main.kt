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
val dispatcher = Executors.newFixedThreadPool(12).asCoroutineDispatcher()

fun main() = runBlocking {
     println("${LocalDateTime.now()} started")

     var firstOctets: ConcurrentHashMap<String, Int>
     val time = measureTime {
          firstOctets = getFirstOctets()
     }
     println("First octets found in ${time.inWholeMinutes} minutes. size ${firstOctets.size}")

     val measureTime = measureTime {
          val count = firstOctets.map { item ->
               async(dispatcher) {
                    try {
                         countAddresses(item.key)
                    } catch (e: Exception) {
                         println("Exception occurred $e")
                         retry { countAddresses(item.key) }
                    }
               }
          }.awaitAll()
               .sum()
          println("Amount of unique addresses is $count")
          println("Errors occurred $errorCounter ")
     }
     println("${LocalDateTime.now()} Found in ${measureTime.inWholeMinutes} minutes")
}

private fun countAddresses( octet: String): Int {
     println("Octet $octet processing started")
     val start = Instant.now().epochSecond

     val hashSet = HashSet<String>(4_000_000)
     val stream = Files.lines(Path.of(file))
     stream.use {
          it.sequential().forEach { x ->
               if (x.startsWith(octet)) {
                    hashSet.add(x)
               }
          }
     }
     val value = hashSet.count()

     val end = Instant.now().epochSecond
     println("Count of ip with first octet $octet is $value. Spent  ${(end - start) / 60} minutes on cycle")
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