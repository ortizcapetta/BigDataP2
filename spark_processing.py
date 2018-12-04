# coding=utf-8
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover
import pyspark.sql.functions as f
from pyspark.sql.functions import split


def start_analysis(spark, hour):
    #load two csvs, one that contains hashtags and another that has tweets
    hashtag = spark.read.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/hashtag.csv')
    tweet = spark.read.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/tweets.csv')
    #hashtag df
    hashtag.createOrReplaceTempView("hashtag")
    #get top 15 hashtags per hour
    hashtags = spark.sql("select _c0 as hashtag, count(*) "
                        "from hashtag "
                        "where _c1 = {} "
                        "group by hashtag "
                        "order by count(*) desc "
                        "limit 15".format(str(hour)))
    #save hashtag info
    hashtags.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/hashtags/hashtagfreq'+str(hour))
    hashtags.show()
    #tweets df
    tweet.createOrReplaceTempView("tweets")
    #get top 15 posters per hour
    uids = spark.sql("select _c0 as uid,_c1 as username, count(*) "
                        "from tweets "
                        "where _c3 = {} "
                        "group by uid,username "
                        "order by count(*) desc "
                        "limit 15".format(str(hour)))
    #save
    uids.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/users/uidfreq'+str(hour))
    uids.show()
    #get instances of those keywords per hour
    keywords = spark.sql("select (case "
                         "when lower(_c2) like '%trump%' then 'trump' "
                         "when lower(_c2) like '%headache%' then 'headache' "
                         "when lower(_c2) like '% flu %' then 'flu' "
                         "when lower(_c2) like '% zika %' then 'zika' "
                         "when lower(_c2) like '%diarrhea%' then 'diarrhea' "
                         "when lower(_c2) like '% ebola %' then 'ebola' "
                         "when lower(_c2) like '%headache%' then 'headache' "
                         "when lower(_c2) like '%measles%' then 'measles' "
                         "else NULL "
                         "end) as keywords,count(*) "
                         "from tweets "
                         "where _c3 = {} "
                         "group by keywords "
                         "order by count(*) desc".format(str(hour)))

    #get the full tweet of those instances of keywords with the exeption of trump
    keywordsfull = spark.sql("select (case "
                         "when lower(_c2) like '%headache%' then _c2 "
                         "when lower(_c2) like '% flu %' then _c2 "
                         "when lower(_c2) like '% zika %' then _c2 "
                         "when lower(_c2) like '%diarrhea%' then _c2 "
                         "when lower(_c2) like '% ebola %' then _c2 "
                         "when lower(_c2) like '%headache%' then _c2 "
                         "when lower(_c2) like '%measles%' then _c2 "
                         "else NULL "
                         "end) as keywords "
                         "from tweets "
                         "where _c3 = {}".format(str(hour)))
    #save both
    keywords.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/keywords/keywords'+str(hour))
    keywordsfull.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/keywords/keywordsfull'+str(hour))

    keywords.show()
    #select all text from tweets
    words = spark.sql("select _c2 from tweets "
                      "where _c3 = {}".format(str(hour)))

    words = words.withColumn('word', f.explode(f.split(f.col('_c2'), ' ')))\

    #english stop words
    stopwords = ["a", "about", "above", "above", "across", "after", "afterwards", "again", "against","all", "almost", "alone", "along", "already", "also","although","always","am",
                "among", "amongst", "amoungst", "amount", "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as", "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from",
                "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie","i", "if", "in", "inc", "indeed",
                "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile",
                "might", "mill","like","im","I'm","et", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often",
                "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six",
                "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever",
                "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole",
                "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"," ","","que","y","."]
    #mostly spanish stopwrods but has stopwords from other languages as well
    sstopwords = ["0","1","2","3","4","5","6","7","8","9",".", "-", "&amp","&amp;","_","..",":","a","des","pas", "à","من","في","و", "é",
                  "actualmente","acuerdo","adelante","ademas","además","adrede","afirmó","agregó","ahi","ahora","ahí",
                  "al","algo","alguna","algunas","alguno","algunos","algún","alli","allí","alrededor","ambos","ampleamos","antano","antaño","ante","anterior","antes","apenas","aproximadamente","aquel","aquella","aquellas","aquello","aquellos","aqui","aquél","aquélla","aquéllas","aquéllos","aquí","arriba","arribaabajo","aseguró","asi","así","atras","aun","aunque","ayer","añadió","aún","b","bajo","bastante","bien","breve","buen","buena","buenas","bueno","buenos","c","cada","casi","cerca","cierta","ciertas","cierto","ciertos","cinco","claro","comentó","como","con","conmigo","conocer","conseguimos",
                  "conseguir","considera","consideró","consigo","consigue","consiguen","consigues","contigo","contra","cosas","creo","cual","cuales","cualquier","cuando","cuanta","cuantas","cuanto","cuantos","cuatro","cuenta","cuál","cuáles","cuándo","cuánta","cuántas","cuánto","cuántos",
                  "cómo","d","da","dado","dan","dar","de","debajo","debe","deben","debido","decir","dejó","del","delante",
                  "demasiado","demás","dentro","deprisa","desde","despacio","despues","después","detras","detrás","dia","dias",
                  "dice","dicen","dicho","dieron","diferente","diferentes","dijeron","dijo","dio","donde","dos","durante","día",
                  "días","dónde","e","ejemplo","el","ella","ellas","ello","ellos","embargo","empleais","emplean","emplear","empleas","empleo","en","encima","encuentra","enfrente","enseguida","entonces","entre","era",
                  "erais","eramos","eran","eras","eres","es","esa","esas","ese","eso","esos","esta","estaba","estabais","estaban","estabas","estad","estada","estadas","estado","estados","estais",
                  "estamos","estan","estando","estar","estaremos","estará","estarán","estarás","estaré","estaréis","estaría","estaríais","estaríamos","estarían","estarías","estas","este","estemos",
                  "esto","estos","estoy","estuve","estuviera","estuvierais","estuvieran","estuvieras","estuvieron","estuviese","estuvieseis","estuviesen","estuvieses","estuvimos","estuviste","estuvisteis","estuviéramos",
                  "estuviésemos","estuvo","está","estábamos","estáis","están","estás","esté","estéis","estén","estés","ex","excepto","existe","existen","explicó","expresó","f","fin","final","fue","fuera","fuerais","fueran","fueras","fueron","fuese","fueseis","fuesen","fueses","fui","fuimos","fuiste",
                  "fuisteis","fuéramos","fuésemos","g","general","gran","grandes","gueno","h","ha","haber","habia","habida","habidas","habido","habidos","habiendo","habla","hablan","habremos","habrá","habrán","habrás","habré","habréis","habría","habríais","habríamos","habrían","habrías","habéis","había","habíais","habíamos","habían","habías","hace","haceis","hacemos","hacen","hacer","hacerlo","haces","hacia","haciendo","hago","han","has","hasta","hay","haya","hayamos","hayan","hayas","hayáis","he","hecho",
                  "hemos","hicieron","hizo","horas","hoy","hube","hubiera","hubierais","hubieran","hubieras","hubieron","hubiese","hubieseis","hubiesen","hubieses","hubimos","hubiste","hubisteis","hubiéramos","hubiésemos","hubo","i","igual","incluso","indicó","informo","informó","intenta","intentais","intentamos","intentan","intentar","intentas","intento","ir","j","junto","k","l","la","lado","largo","las","le","lejos","les","llegó","lleva","llevar","lo","los","luego","lugar","m","mal","manera","manifestó","mas","mayor","me","mediante","medio","mejor","mencionó","menos","menudo","mi","mia","mias","mientras","mio","mios","mis","misma","mismas","mismo","mismos","modo","momento","mucha","muchas","mucho","muchos","muy","más","mí","mía","mías","mío","míos","n","nada","nadie","ni","ninguna","ningunas","ninguno",
                  "ningunos","ningún","no","nos","nosotras","nosotros","nuestra","nuestras","nuestro","nuestros","nueva","nuevas","nuevo","nuevos","nunca","o","ocho","os","otra","otras","otro","otros","p","pais","para","parece","parte","partir","pasada","pasado","paìs","peor","pero","pesar","poca","pocas","poco","pocos","podeis","podemos","poder","podria","podriais","podriamos","podrian","podrias","podrá","podrán","podría","podrían","poner","por","por qué","porque","posible","primer","primera","primero","primeros","principalmente","pronto","propia","propias","propio","propios","proximo","próximo","próximos","pudo","pueda","puede","pueden","puedo","pues",
                  "q","qeu","que","quedó","queremos","quien","quienes","quiere","quiza","quizas","quizá","quizás","quién","quiénes","qué","r","raras","realizado","realizar","realizó","repente","respecto","s","sabe","sabeis","sabemos","saben","saber","sabes","sal","salvo","se","sea",
                  "seamos","sean","seas","segun","segunda","segundo","según","seis","ser","sera","seremos","será","serán","serás","seré","seréis","sería","seríais","seríamos","serían","serías","seáis","señaló","si","sido","siempre","siendo","siete","sigue","siguiente","sin","sino","sobre","sois","sola","solamente","solas","solo","solos","somos","son","soy","soyos","su","supuesto","sus","suya","suyas","suyo","suyos","sé","sí","sólo","t","tal","tambien","también","tampoco","tan","tanto","tarde","te","temprano","tendremos","tendrá","tendrán","tendrás","tendré","tendréis","tendría","tendríais","tendríamos","tendrían","tendrías","tened","teneis","tenemos","tener","tenga","tengamos","tengan","tengas","tengo","tengáis","tenida","tenidas","tenido","tenidos","teniendo","tenéis","tenía","teníais","teníamos",
                  "tenían","tenías","tercera","ti","tiempo","tiene","tienen","tienes","toda","todas","todavia","todavía","todo","todos","total","trabaja","trabajais","trabajamos","trabajan","trabajar","trabajas","trabajo","tras","trata","través",
                  "tres","tu","tus","tuve","tuviera","tuvierais","tuvieran","tuvieras","tuvieron","tuviese","tuvieseis","tuviesen","tuvieses","tuvimos","tuviste","tuvisteis","tuviéramos","tuviésemos","tuvo","tuya","tuyas","tuyo","tuyos","tú","u","ultimo","un","una","unas","uno","unos","usa","usais","usamos","usan","usar","usas","uso","usted","ustedes"
                  ,"va","vais","valor","vamos","van","varias","varios","vaya","veces","ver","verdad","verdadera","verdadero","vez","vosotras","vosotros","voy","vuestra","vuestras","vuestro","vuestros","w","x","y","ya","yo","z","él","éramos","ésa","ésas","ése","ésos","ésta",
                  "éstas","éste","éstos","última","últimas","último","últimos"]
    fullstopwords = stopwords + sstopwords
    #do a bunch of stuff to get the wods
    words = words.withColumn("words", split("word", "\s+"))
    remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=fullstopwords)
    words = remover.transform(words)
    words = words.groupBy('filtered').count().sort('count',ascending=False)
    words = words.withColumn('filtered', f.concat_ws('|', 'filtered')).limit(15)
    words.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/words/wordfreq'+str(hour))
    words.show()


def top_users12(spark, hour):
    tweet = spark.read.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/tweets.csv')
    tweet.createOrReplaceTempView("tweets")
    top = hour * 12
    bottom = top + 12
    uids = spark.sql("select _c0 as uid,_c1 as username, count(*) "
                     "from tweets "
                     "where _c3 >= {} and _c3 <= {} "
                     "group by uid,username "
                     "order by count(*) desc "
                     "limit 15".format(str(top),str(bottom)))
    uids.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/users/top12/top12-' + str(hour))
    uids.show()
def fulltweets(spark):
    tweet = spark.read.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/tweets.csv')
    tweet.createOrReplaceTempView("tweets")
    keywordsfull = spark.sql("select (case "
                             "when lower(_c2) like '%headache%' then _c2 "
                             "when lower(_c2) like '% flu %' then _c2 "
                             "when lower(_c2) like '% zika %' then _c2 "
                             "when lower(_c2) like '%diarrhea%' then _c2 "
                             "when lower(_c2) like '% ebola %' then _c2 "
                             "when lower(_c2) like '%headache%' then _c2 "
                             "when lower(_c2) like '%measles%' then _c2 "
                             "else NULL "
                             "end) as keywords "
                             "from tweets "
                             )
    keywordsfull.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/keywords_full1')

    keywords = spark.sql("select (case "
                         "when lower(_c2) like '%trump%' then 'trump' "
                         "when lower(_c2) like '%headache%' then 'headache' "
                         "when lower(_c2) like '% flu %' then 'flu' "
                         "when lower(_c2) like '% zika %' then 'zika' "
                         "when lower(_c2) like '%diarrhea%' then 'diarrhea' "
                         "when lower(_c2) like '% ebola %' then 'ebola' "
                         "when lower(_c2) like '%headache%' then 'headache' "
                         "when lower(_c2) like '%measles%' then 'measles' "
                         "else NULL "
                         "end) as keywords,count(*) "
                         "from tweets "
                         "group by keywords ")
    keywords.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/keywords1')
    uids = spark.sql("select _c0 as uid,_c1 as username, count(*) "
                     "from tweets "
                     "group by uid,username "
                     "order by count(*) desc "
                     "limit 15")
    # save
    uids.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/uids1')

    hashtag = spark.read.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/hashtag.csv')
    hashtag.createOrReplaceTempView("hashtag")
    hashtags = spark.sql("select _c0 as hashtag, count(*) "
                         "from hashtag "
                         "group by hashtag "
                         "order by count(*) desc "
                         "limit 15")
    # save hashtag info
    hashtags.coalesce(1).write.csv('/home/alejandra/Documents/CIIC5995/BigDataP2/results/hashtags1')

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    hours = 24
    for hour in range(0, hours):
        print("HOUR : " + str(hour))
        start_analysis(spark, hour)
    for hour in range(0, int(hours/12)):
        print("HOURS RANGE: " + str(hour * 12) + " to " + str((hour*12)+12))
        top_users12(spark, hour)
    #full tweets is data of the whole day
    fulltweets(spark)

