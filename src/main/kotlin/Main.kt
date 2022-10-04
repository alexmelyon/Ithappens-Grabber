import kotlinx.coroutines.*
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import java.io.File
import java.net.URL
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.*

@OptIn(DelicateCoroutinesApi::class)
fun main(args: Array<String>) {
    File("saved").mkdirs()
    val numCpus = Runtime.getRuntime().availableProcessors()
    val context = newFixedThreadPoolContext(numCpus, "DownloaderPool")
    val scope = CoroutineScope(context)
    val connection = initSqlite()
    runBlocking {
        val deferreds = mutableListOf<Deferred<Unit>>()
        for (page in 1487 downTo 1) {
            val def = scope.async {
                downloadPage(page)
                val parsedList = parsePage(page, connection)
                parsedList.forEach { storePage(it, connection) }
            }
            deferreds.add(def)
        }
        deferreds.awaitAll()
    }
    connection.close()
}

fun initSqlite(): Connection {
    val connection = DriverManager.getConnection("jdbc:sqlite:ithappens.sqlite")
    connection.createStatement()
        .execute(
            "CREATE TABLE IF NOT EXISTS stories (" +
                    "storyId INTEGER PRIMARY KEY," +
                    "title STRING," +
                    "datetime INTEGER," +
                    "dateStr STRING," +
                    "tags STRING," +
                    "text STRING," +
                    "likes INTEGER" +
                    ")"
        )
    return connection
}

fun storePage(page: IthappensPage, connection: Connection) {
    println("Store ${page.storyId}")
    val stm = connection.prepareStatement("INSERT INTO stories (storyId, title, datetime, dateStr, tags, text, likes) VALUES (?, ?, ?, ?, ?, ?, ?)")
    stm.setInt(1, page.storyId)
    stm.setString(2, page.title)
    stm.setLong(3, page.datetime)
    stm.setString(4, page.dateStr)
    stm.setString(5, page.tags.joinToString(", "))
    stm.setString(6, page.text)
    stm.setInt(7, page.likes)
    stm.executeUpdate()
    stm.close()
}

data class IthappensPage(
    val storyId: Int,
    val title: String,
    val datetime: Long,
    val dateStr: String,
    val tags: List<String>,
    val text: String,
    val likes: Int
)

fun parsePage(page: Int, connection: Connection): List<IthappensPage> {
    val containerSelector = "body>div>div>div>div.story"
    val storyIdSelector = "div.id > span"
    val titleSelector = "h2 > a"
    val datetimeSelector = "div.meta > time"
    val dateSelector = "div.meta > div.date-time"
    val tagsSelector = """div.meta > div > ul > li > a"""
    val textSelector = "div.text > p"
    val likesSelector = "div.actions > div.button-group.like > div > div"

    val f = File("saved/$page.html")
    val document = Jsoup.parse(f)
    val elements = document.body().select(containerSelector)
        .getElements()
//    println(elements.size)
    val parsed = mutableListOf<IthappensPage>()
    for (elem in elements) {
        val storyId = elem.select(storyIdSelector)
            .getElements()[0]
            .html()
            .toInt()
        if (isKeyExists(storyId, connection)) {
            continue
        }
//        println("StoryId $storyId")
        val title = elem.select(titleSelector)
            .html()
//        println("Title $title")

        val datetime = elem.select(datetimeSelector)
            .getElements()
            .getOrNull(0)
            ?.attr("datetime")
            ?.let { Instant.parse(it) }
            ?.epochSecond
        var date = ""
        if (datetime == null) {
            date = elem.select(dateSelector)
                .getElements()[0]
                .html()
        }

//        println("Date $date")
        val tags = elem.select(tagsSelector)
            .getElements()
            .map { it.html() }
//        println("Tags $tags")
        val text = elem.select(textSelector)
            .getElements()
            .map { it.html() }
            .joinToString("\n")
//        println(text)
        val likes = elem.select(likesSelector)
            .getElements()[0]
            .html()
            .toInt()
//        println("Likes $likes")
        parsed.add(
            IthappensPage(
                storyId,
                title,
                datetime ?: 0L,
                date,
                tags,
                text,
                likes
            )
        )
    }
    return parsed
}

fun isKeyExists(storyId: Int, connection: Connection): Boolean {
    val stm = connection.createStatement()
    stm.execute("SELECT COUNT(storyId) FROM stories WHERE storyId=$storyId")
    val res = stm.resultSet.getInt(1)
    return res > 0
}

fun Elements.getElements(): List<Element> {
    return (0 until size).mapIndexed { i, e -> get(i) }
}

fun downloadPage(page: Int) {
    val filename = "saved/$page.html"
    if (File(filename).exists()) {
        return
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
        val e = Exception("Page $page", t)
        e.printStackTrace()
    }
}