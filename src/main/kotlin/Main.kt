import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap

const val file = "C:\\Users\\lifte\\Downloads\\ip_addresses\\ip_addresses"

fun main()  {
     println("${LocalDateTime.now()} started")
     val start = Instant.now().epochSecond

     val firstOctets = ConcurrentHashMap<String, Int>()
     firstOctets.put("1.", 0)
     firstOctets.put("0.", 0)
//          getFirstOctetsMap()

//     println("${LocalDateTime.now()} Size of firstOctets ${firstOctets.size}")
     for(i in firstOctets.keys()) {
          val start = Instant.now().epochSecond
          val hashSet = ConcurrentHashMap.newKeySet<String>(4_000_000)
          val stream = Files.lines(Path.of(file))
          stream.use {
               it.parallel().forEach { x ->
                    if(x.startsWith(i)) {
                         hashSet.add(x)
                    }
               }
          }
          val value = hashSet.count()
          firstOctets.put(i, value)
          println("Count of ip with first octet $i is $value")
          val end = Instant.now().epochSecond
          println("Time spent on cycle ${(end - start) / 60}")
     }
     val uniqueIps = firstOctets.values.sum()

     val end = Instant.now().epochSecond
     val time = (end - start) / 60
     println("Size of unique ips: $uniqueIps  lines. Minutes spent: $time")

}

private fun getFirstOctetsMap(): ConcurrentHashMap<String, Int> {
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