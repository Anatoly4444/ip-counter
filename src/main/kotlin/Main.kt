import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

fun main()  {
     val start = Instant.now().epochSecond
     val file = "C:\\Users\\lifte\\Downloads\\ip_addresses\\ip_addresses"
     val stream = Files.lines(Path.of(file))

//     val map = HashMap<Int, String>(16_582_000)
     val firstOctets = ConcurrentHashMap<Int, Int>()

//     launch {
          stream.use {
               it.parallel().forEach { x ->
                    val firstOctetOfIp = x.split(".").get(0).toInt()
                    firstOctets.put(firstOctetOfIp, 0)
               }
          }
//     }
//     while(firstOctets.isEmpty()) {
//          Thread.sleep(2000)
//     }
     println("Size of firstOctets ${firstOctets.size}")
     val map = ConcurrentHashMap<Int, MutableSet<String>>(firstOctets.size)
     for(i in firstOctets.keys()) {
          val start = Instant.now().epochSecond
          val stream = Files.lines(Path.of(file))
          stream.use {
               it.parallel().forEach { x ->
                    val firstOctetOfIp = x.split(".").get(0).toInt()
                    if(i == firstOctetOfIp) {
                         map.compute(firstOctetOfIp) {_, v ->
                              if(v == null) {
                                   return@compute mutableSetOf<String>(x)
                              } else {
                                   v.add(x)
                                   return@compute v
                              }
                         }
                    }
               }
          }
          val countOfIp = map.get(i)?.count()
          if (countOfIp != null) {
               firstOctets.put(i, countOfIp)
               map.remove(i)
          }
          println("Count of ip with first octet $i is $countOfIp")
          val end = Instant.now().epochSecond
          println("Time spent on cycle ${(end - start) / 60}")
     }
     val uniqueIps = firstOctets.values.sum()

     val end = Instant.now().epochSecond
     val time = (end - start) / 60
     println("Size of unique ips: $uniqueIps  lines. Minutes spent: $time")

}