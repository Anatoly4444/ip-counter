import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap

const val file = "C:\\Users\\lifte\\Downloads\\ip_addresses\\ip_addresses"
val firstOctets = ConcurrentHashMap<String, Int>()

fun main() = runBlocking {
     println("${LocalDateTime.now()} started")
     val start = Instant.now().epochSecond

//     putFirstOctetsMap()
          ConcurrentHashMap<String, Int>()
     firstOctets.put("1.", 0)
     firstOctets.put("0.", 0)
//     firstOctets.put("0.", 0)

     println("${LocalDateTime.now()} Size of firstOctets ${firstOctets.size}")
     val iterator = firstOctets.keys().asIterator()
     while(iterator.hasNext()) {
          val job1 = launch(Dispatchers.IO) {
               countAddresses(iterator.next())
          }
          val job2 = launch(Dispatchers.IO) {
               countAddresses(iterator.next())
          }
          job1.join()
          job2.join()
     }
     val allUniqueAddresses = firstOctets.values.sum()

     val end = Instant.now().epochSecond
     val time = (end - start) / 60
     println("Size of unique ips: $allUniqueAddresses  lines. Minutes spent: $time")

}

private fun countAddresses(octet: String) {
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
     firstOctets.put(octet, value)

     println("Count of ip with first octet $octet is $value")
     val end = Instant.now().epochSecond
     println("Time spent on cycle ${(end - start) / 60}")
}

private fun putFirstOctetsMap() {
     val stream = Files.lines(Path.of(file))
     stream.use {
          it.parallel().forEach { x ->
               val firstOctetOfIp = x.split(".").get(0)
               firstOctets.put("$firstOctetOfIp.", 0)
          }
     }
}