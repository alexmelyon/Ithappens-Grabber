import java.io.File
import java.net.URL
import java.net.URLConnection
import java.util.*

fun main(args: Array<String>) {
//    val page = 1484
//    val baseUrl = "https://web.archive.org/web/20220120110021/https://ithappens.me/page/$page"

    File("saved").mkdirs()
    for (page in 1484 downTo 0) {
        val filename = "saved/$page.html"
        if (File(filename).exists()) {
            continue
        }
        println("DOWNLOAD $page ${Date()}")
        try {
            val f = File(filename)
            f.setWritable(true)
            URL("https://web.archive.org/web/20220120110021/https://ithappens.me/page/$page")
                .openConnection()
                .getInputStream()
                .readAllBytes()
                .let { f.writeBytes(it) }

        } catch (t: Throwable) {
            t.printStackTrace()
        }
    }
}