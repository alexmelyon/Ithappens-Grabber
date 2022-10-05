package com.github.alexmelyon.ithappens_grabber

import stemmer.russianStemmer
import kotlinx.coroutines.*
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import java.io.File
import java.io.InputStream
import java.net.URL
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.*
import kotlin.system.exitProcess

val connection = DriverManager.getConnection("jdbc:sqlite:ithappens.sqlite")
val stemmer = russianStemmer()
val scope = initScope()

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        showUsage()
        exitProcess(0)
    }
    initSqlite()
    if("--dropsearch" in args) {
        dropSearchDB()
    }
    initSearchDB()

    runBlocking {
        val deferreds = mutableListOf<Deferred<Unit>>()
        for (page in 1487 downTo 1) {
            val def = scope.async {

                val input = if ("--download" in args) {
                    downloadPage(page).onSuccess {
                        storeFile("saved/$page.html", it)
                    }.getOrNull()
                } else if ("--saved" in args) {
                    getPage(page)
                } else null
                input ?: return@async
                val stories = parsePage(input)
                stories.forEach {
                    storeDB(it)
                }
            }
            deferreds.add(def)
        }
        deferreds.awaitAll()

        val maxStoryId = getMaxStoryId()
        for (storyId in 1..maxStoryId) {
            val story = getStory(storyId)
            story ?: continue
            updateSearchDB(story)
        }
    }

    if ("--search" in args) {
        val words = args.toList()
            .subList(args.indexOf("--search"), args.size)
            .joinToString(" ")
        search(words)
    }

    connection.close()
}

fun showUsage() {
    println(
        "Usage:\n" +
                "\t--search %s  - Search articles including words\n" +
                "\t--download   - Download all pages from Wayback Machine\n" +
                "\t--saved      - Read htmls from 'saved' folder\n" +
                "\t--dropsearch - Drop table search\n"
    )
}

fun getPage(page: Int): InputStream? {
    val f = File("saved/$page.html")
    if (!f.exists() || !f.isFile) {
        return null
    }
    return f.inputStream().buffered()
}

fun dropSearchDB() {
    connection.createStatement().apply {
        execute("DROP TABLE IF EXISTS search")
    }
}

fun <R> time(block: () -> R): R {
    val now = System.currentTimeMillis()
    val res = block.invoke()
    val end = System.currentTimeMillis() - now
    println("Time ${end.toDouble() / 1000} sec")
    return res
}

fun search(words: String): List<Pair<Int, Int>> {
    val documentToCount = mutableMapOf<Int, Int>()
    words.split(" ")
        .filter { it.isNotBlank() }
        .map { word ->
            connection.createStatement().run {
                execute("SELECT document, wordCount FROM search WHERE word='$word' ORDER BY wordCount DESC, document DESC")
                val rs = resultSet
                while (rs.next()) {
                    val doc = rs.getInt(1)
                    val wordCount = rs.getInt(2)
                    val c = documentToCount.getOrDefault(doc, 0)
                    documentToCount[doc] = c + wordCount
                }
                documentToCount
            }
        }
    return documentToCount.toList().sortedByDescending { it.first }
}

@OptIn(DelicateCoroutinesApi::class)
fun initScope(): CoroutineScope {
    val numCpus = Runtime.getRuntime().availableProcessors()
    val context = newFixedThreadPoolContext(numCpus, "DownloaderPool")
    return CoroutineScope(context)
}

fun getMaxStoryId(): Int {
    return connection.createStatement().run {
        execute("SELECT MAX(storyId) FROM stories")
        resultSet.getInt(1)
    }
}

fun String.stemmed(): String {
    stemmer.current = this
    stemmer.stem()
    return stemmer.current
}

fun invertedIndex(story: IthappensStory): List<Triple<String, Int, Int>> {
    val wordCount = wordCount(story.text)
        .map { it.key.lowercase() to it.value }
    val res = wordCount.map { Triple(it.first.stemmed(), story.storyId, it.second) }
    return res
}

fun initSearchDB() {
    connection.createStatement().run {
        execute(
            "CREATE TABLE IF NOT EXISTS search(" +
                    "word STRING," +
                    "document INTEGER," +
                    "wordCount INTEGER" +
                    ")"
        )
    }
}

fun updateSearchDB(story: IthappensStory) {

    val wordDocumentCount = invertedIndex(story)

    wordDocumentCount.forEach { (word, document, count) ->
        val stm = connection.prepareStatement(
            "INSERT INTO search " +
                    "(word, document, wordCount) " +
                    "VALUES (?, ?, ?)"
        )
        stm.setString(1, word)
        stm.setInt(2, document)
        stm.setInt(3, count)
        stm.executeUpdate()
        stm.close()
    }
}

fun wordCount(text: String): Map<String, Int> {
    val words = text.split("[^A-Za-zА-Яа-я0-9']+".toRegex())
    val map = mutableMapOf<String, Int>()
    words.forEach { map.put(it, 1 + map.getOrDefault(it, 0)) }
    return map
}

fun getStory(storyId: Int): IthappensStory? {
    val stm = connection.createStatement().apply {
        execute("SELECT * FROM stories WHERE storyId=$storyId")
    }
    val story = with(stm) {
        val rs = resultSet
        IthappensStory(
            rs.getInt(1),
            rs.getString(2),
            rs.getLong(3),
            rs.getString(4).split(", "),
            rs.getString(5),
            rs.getInt(6)
        )
    }
    stm.close()
    return story
}

fun initSqlite(): Connection {
    connection.createStatement().apply {
        execute(
            "CREATE TABLE IF NOT EXISTS stories (" +
                    "storyId INTEGER PRIMARY KEY," +
                    "title STRING," +
                    "datetime INTEGER," +
                    "tags STRING," +
                    "text STRING," +
                    "likes INTEGER" +
                    ")"
        )
    }
    return connection
}

fun storeDB(page: IthappensStory) {
    if (isKeyExists(page.storyId)) {
        return
    }
    println("STORE ${page.storyId}")
    val stm = connection.prepareStatement(
        "INSERT INTO stories " +
                "(storyId, title, datetime, tags, text, likes) " +
                "VALUES (?, ?, ?, ?, ?, ?)"
    )
    stm.setInt(1, page.storyId)
    stm.setString(2, page.title)
    stm.setLong(3, page.datetime)
    stm.setString(4, page.tags.joinToString(", "))
    stm.setString(5, page.text)
    stm.setInt(6, page.likes)
    stm.executeUpdate()
    stm.close()
}

data class IthappensStory(
    val storyId: Int,
    val title: String,
    val datetime: Long,
    val tags: List<String>,
    val text: String,
    val likes: Int
)

fun parsePage(input: InputStream): List<IthappensStory> {
    val containerSelector = "body > div > div > div > div.story"
    val storyIdSelector = "div.id > span"
    val titleSelector = "h2 > a"
    val datetimeSelector = "div.meta > time"
    val dateSelector = "div.meta > div.date-time"
    val tagsSelector = """div.meta > div > ul > li > a"""
    val textSelector = "div.text > p"
    val likesSelector = "div.actions > div.button-group.like > div > div"

    val document = Jsoup.parse(String(input.readAllBytes(), Charsets.UTF_8))
    val elements = document.body().select(containerSelector)
        .getElements()
//    println(elements.size)
    val parsed = mutableListOf<IthappensStory>()
    for (elem in elements) {
        val storyId = elem.select(storyIdSelector)
            .getElements()[0]
            .html()
            .toInt()
//        println("StoryId $storyId")
        val title = elem.select(titleSelector)
            .html()
//        println("Title $title")

        var datetime = elem.select(datetimeSelector)
            .getElements()
            .getOrNull(0)
            ?.attr("datetime")
            ?.let { Instant.parse(it) }
            ?.epochSecond
        if (datetime == null) {
            val dateStr = elem.select(dateSelector)
                .getElements()[0]
                .html()
            datetime = parseDate(dateStr)
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
            IthappensStory(
                storyId,
                title,
                datetime,
                tags,
                text,
                likes
            )
        )
    }
    return parsed
}

fun parseDate(dateStr: String): Long {
    val values = """(\d+) ([а-я]+) (\d+), (\d+):(\d+)""".toRegex()
        .find(dateStr)!!
        .groupValues
    val day = values[1].toInt()
    val month = values[2]
    val year = values[3].toInt()
    val hour = values[4].toInt()
    val minutes = values[5].toInt()
    val cal = Calendar.getInstance()
    cal.timeZone = TimeZone.getTimeZone("Europe/Moscow")
    cal.set(Calendar.DAY_OF_MONTH, day)
    val monthN = when (month.substring(0, 3)) {
        "янв" -> 0
        "фев" -> 1
        "мар" -> 2
        "апр" -> 3
        "мая" -> 4
        "июн" -> 5
        "июл" -> 6
        "авг" -> 7
        "сен" -> 8
        "окт" -> 9
        "ноя" -> 10
        "дек" -> 11
        else -> null
    }
    cal.set(Calendar.MONTH, monthN!!)
    cal.set(Calendar.YEAR, year)
    cal.set(Calendar.HOUR_OF_DAY, hour)
    cal.set(Calendar.MINUTE, minutes)
    val date = cal.time
    return date.time
}

fun isKeyExists(storyId: Int): Boolean {
    val stm = connection.createStatement()
    stm.execute("SELECT COUNT(storyId) FROM stories WHERE storyId=$storyId")
    val res = stm.resultSet.getInt(1)
    return res > 0
}

fun Elements.getElements(): List<Element> {
    return (0 until size).mapIndexed { i, e -> get(i) }
}

fun downloadPage(page: Int): Result<InputStream> {
    println("DOWNLOAD $page ${Date()}")
    try {
        val input = URL("https://web.archive.org/web/20220120110021/https://ithappens.me/page/$page")
            .openConnection()
            .getInputStream()
            .buffered()
        return Result.success(input)
    } catch (t: Throwable) {
        t.printStackTrace()
        return Result.failure(t)
    }
}

fun storeFile(filename: String, input: InputStream): Result<Boolean> {
    val f = File(filename)
    if (f.exists()) {
        return Result.success(true)
    }
    try {
        f.mkdirs()
        f.setWritable(true)
        val out = f.outputStream().buffered()
        input.copyTo(out)
        return Result.success(true)
    } catch (t: Throwable) {
        t.printStackTrace()
        return Result.failure(t)
    }
}