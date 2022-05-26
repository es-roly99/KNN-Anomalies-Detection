package knn

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.broadcast._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.storage.StorageLevel
import AuxiliaryClass._

object Algorithm {

    var k = 0
    var p = 0.0

    //Creando objeto logger necesario para almacenamiento de trazas de la aplicacion
    //private final val mylogger: Logger = LogManager.getLogger("KnnwoOD")

    /**
     * exce es la función que se encarga de ejecutar el algoritmo KNNW_BigData.
     *
     * @param data  es un Dataset de Row que contiene la base de datos a la que se le aplica el algoritmo KNNW_BigData
     * @param spark es el SparkSession de la aplicación
     * @param K     es un valor entero que representa la cantidad de vecinos cercanos para una instancia
     * @param P     es un valor Double que representa el por ciento de instancias que de mayor índice de anomalías que se seleccionan para la segunda fase del algoritmo.
     * @return Retorna un Dataset que contiene el identificador único de las tuplas y su índice de anomalía correspondiente
     */

    def train(data: Dataset[Row], spark: SparkSession, K: Int, P: Double, ID: String = "ID"): Dataset[Clasificacion] = {

        k = K
        p = P
        val cantTupla = data.count()
        import spark.implicits._

        println("Parseando tuplas")
        println("**********************************************")
        println("              PARSEANDO TUPLAS")
        println("**********************************************")
        val ds = data.map { row => parseTupla(row, spark, ID) }
        println("Ejecutando Fase1")

        println("**********************************************")
        println("                    FASE1")
        println("**********************************************")
        val dsfase1 = ds.mapPartitions { x => fase1(x.toArray, spark) }.persist(StorageLevel.MEMORY_AND_DISK_SER)
        val filtro = (cantTupla * p).toInt

        println("Ordenando Fase1")

        println("**********************************************")
        println("                ORDENANDO FASE1")
        println("**********************************************")
        val lim = dsfase1.sort(col("ia").desc).limit(filtro).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val broadcast = spark.sparkContext.broadcast(lim.collect())

        println("Ejecutando Fase2")

        println("**********************************************")
        println("                    FASE2")
        println("**********************************************")
        val outlier = fase2(broadcast, dsfase1, spark).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val bc = spark.sparkContext.broadcast(outlier.collect())

        println("Ejecutando Update")

        println("**********************************************")
        println("                    UPDATE")
        println("**********************************************")
        val resultado = dsfase1.mapPartitions { iter => update(bc, iter.toArray, spark) }.persist(StorageLevel.MEMORY_AND_DISK_SER)
        val classData = clasificar(resultado, spark)

        resultado.count()
        outlier.unpersist()
        lim.unpersist()
        dsfase1.unpersist()
        ds.unpersist()
        println("Ejecutado algoritmo de deteccion de anomalias")
        classData
    }

    def clasificar(data: Dataset[Resultado], spark: SparkSession): Dataset[Clasificacion] = {

        import spark.implicits._
        val Stats = data.select("ia").describe().drop("summary").collect().slice(1, 3)
        var struct = data.schema.add(StructField("tipo", StringType))
        val classData = data.map { x =>
            var tipo = ""
            val value = x.ia
            val mean = Stats(0).getString(0).toDouble
            val StDev = Stats(1).getString(0).toDouble
            if (value > (mean + 3 * StDev))
                tipo = "anomalia"
            else
                tipo = "normal"
            Clasificacion(x.ID, x.ia, x.data, tipo)
        }
        classData
    }

    /**
     * fase2 es la función que se encarga de ajustar el índice de anomalía de las instancias seleccionadas para la segunda fase del algoritmo KNNW_BigData. Inicialmente esta función determina
     * las vecindades, en todas las particiones de la base de datos, de las instancias seleccionadas. Posteriormente se agrupan las vecindades por el identificador de las instancias.
     * Luego se reducen estas vecindades en una sola con los k vecinos más cercanos de toda la base de datos. Por último, se define a partir de la nueva vecindad de la instancia el nuevo índice de anomalía.
     *
     * @param lista es un Broadcast que contiene un arreglo de TuplaFase1. El arreglo representa las P instancias de mayor índice de anomalías seleccionadas para ajustar sus
     *              respectivos índices. El tipo de dato Broadcast permite al programador  mantener una variable de solo lectura almacenada en caché en cada máquina en lugar de enviar una
     *              copia de ella con tareas. Spark distribuye variables Broadcast utilizando algoritmos de difusión eficientes para reducir el costo de comunicación
     * @param spark es el SparkSession de la aplicación
     * @param rdd   es un Dataset de tipo TuplaFase1 que representa la base de datos asignada al KNNW_BigData
     * @return Retorna un Dataset de tipo TuplaFase1. Este Dataset es el conjunto de datosseleccionados para la segunda fase con sus índices de anomalías ajustados.
     */
    def fase2(lista: Broadcast[Array[(TuplaFase1)]], rdd: Dataset[TuplaFase1], spark: SparkSession): Dataset[TuplaFase1] = {

        import spark.implicits._
        val result = rdd.mapPartitions { iterator =>
            val arr = iterator.toArray
            val r = lista.value.map { x =>
                var l = Array[Double]()
                val iter = arr.aggregate(l)((v1, v2) => insert(distanceds(x.valores.toArray, v2.valores.toArray, spark), v1, spark), (p, set) => insertAll(p, set, spark))
                TuplaFase2(x.id, x.valores, iter)
            }
            r.iterator
        }
        val reduce = result.groupByKey(_.id).reduceGroups((a, b) => TuplaFase2(a.id, a.valores, insertAll(a.distancias.toArray, b.distancias.toArray, spark).toSeq))

        val maper = reduce.map { f => TuplaFase1(f._1, f._2.valores, IA(f._2.distancias.toArray, spark)) }
        maper
    }

    /**
     * fase2Banco es la función que se encarga de ajustar el índice de anomalía de las instancias seleccionadas para la segunda fase del algoritmo KNNW_BigData. Inicialmente esta función determina
     * las vecindades, en todas las particiones de la base de datos, de las instancias seleccionadas. Posteriormente se agrupan las vecindades por el identificador de las instancias.
     * Luego se reducen estas vecindades en una sola con los k vecinos más cercanos de toda la base de datos. Por último, se define a partir de la nueva vecindad de la instancia el nuevo índice de anomalía.
     *
     * @param lista es un Broadcast que contiene un arreglo de TuplaFase1Banco. El arreglo representa las P instancias de mayor índice de anomalías seleccionadas para ajustar sus
     *              respectivos índices. El tipo de dato Broadcast permite al programador  mantener una variable de solo lectura almacenada en caché en cada máquina en lugar de enviar una
     *              copia de ella con tareas. Spark distribuye variables Broadcast utilizando algoritmos de difusión eficientes para reducir el costo de comunicación
     * @param spark es el SparkSession de la aplicación
     * @param rdd   es un Dataset de tipo TuplaFase1Banco que representa la base de datos asignada al KNNW_BigData
     * @return Retorna un Dataset de tipo TuplaFase1Banco. Este Dataset es el conjunto de datosseleccionados para la segunda fase con sus índices de anomalías ajustados.
     */
    def fase2Banco(lista: Broadcast[Array[(TuplaFase1Banco)]], rdd: Dataset[TuplaFase1Banco], spark: SparkSession): Dataset[TuplaFase1Banco] = {

        import spark.implicits._

        val result = rdd.mapPartitions { iterator =>
            val arr = iterator.toArray
            var size = lista.value.length
            var por = 0.toDouble
            var mil = 1.toDouble
            val r = lista.value.map { x =>

                var l = Array[Double]()
                val iter = arr.aggregate(l)((v1, v2) => insert(distancedsBanco(x, v2, spark), v1, spark), (p, set) => insertAll(p, set, spark))
                por = por + 1
                var last = 0
                if (por / 1000.toDouble > mil) {
                    mil = mil + 1

                    if ((por / size * 100).round.toInt > last) {
                        last = (por / size * 100).round.toInt
                    }

                }
                TuplaFase2Banco(x.id, x.mes, x.importe, iter)
            }
            r.iterator
        }
        val reduce = result.groupByKey(_.id).reduceGroups((a, b) => TuplaFase2Banco(a.id, a.mes, a.importe, insertAll(a.distancias.toArray, b.distancias.toArray, spark).toSeq))
        val maper = reduce.map { f => TuplaFase1Banco(f._2.id, f._2.mes, f._2.importe, IA(f._2.distancias.toArray, spark)) }
        maper
    }

    /** updateBanco es una función que actualiza los índices de anomalías de las instancias que fueron seleccionadas para la segunda fase en la partición en que se encuentran.
     *
     * @param lista es un Broadcast que contiene un arreglo de TuplaFase1Banco. El arreglo representa las P instancias de mayor índice de anomalías seleccionadas con sus
     *              respectivos índices ajustados. El tipo de dato Broadcast permite al programador  mantener una variable de solo lectura almacenada en caché en cada máquina en lugar de enviar una
     *              copia de ella con tareas. Spark distribuye variables Broadcast utilizando algoritmos de difusión eficientes para reducir el costo de comunicación
     * @param spark es el SparkSession de la aplicación
     * @param rdd   es un arreglo de tipo TuplaFase1Banco que representa una partición de la base de datos asignada al KNNW_BigData
     * @return Retorna un iterador de tipo TuplaFase1Banco que representa la partición con las instancias actualizadas.
     */
    def updateBanco(lista: Broadcast[Array[(TuplaFase1Banco)]], rdd: Array[TuplaFase1Banco], spark: SparkSession): Iterator[TuplaFase1Banco] = {

        var pos = 0
        val result = rdd.map { iterator =>
            var r = iterator
            var lis = lista.value
            var encontrado = false
            var i = 0
            while (i < lis.length && !encontrado) {

                if (lis.apply(i).id == iterator.id) {
                    r = lis.apply(i)
                    encontrado = true
                }

                i = i + 1
            }
            pos = pos + 1
            r
        }
        result.iterator
    }

    /** update es una función que actualiza los índices de anomalías de las instancias que fueron seleccionadas para la segunda fase en la partición en que se encuentran.
     *
     * @param lista es un Broadcast que contiene un arreglo de TuplaFase1. El arreglo representa las P instancias de mayor índice de anomalías seleccionadas con sus
     *              respectivos índices ajustados. El tipo de dato Broadcast permite al programador  mantener una variable de solo lectura almacenada en caché en cada máquina en lugar de enviar una
     *              copia de ella con tareas. Spark distribuye variables Broadcast utilizando algoritmos de difusión eficientes para reducir el costo de comunicación
     * @param spark es el SparkSession de la aplicación
     * @param rdd   es un arreglo de tipo TuplaFase1 que representa una partición de la base de datos asignada al KNNW_BigData
     * @return Retorna un iterador de tipo Resultado que representa la partición con las instancias actualizadas.
     */
    def update(lista: Broadcast[Array[(TuplaFase1)]], rdd: Array[TuplaFase1], spark: SparkSession): Iterator[Resultado] = {

        var pos = 0
        val result = rdd.map { iterator =>
            var iter = iterator
            var lis = lista.value
            var encontrado = false
            var i = 0
            while (i < lis.length && !encontrado) {

                if (lis.apply(i).id == iterator.id) {
                    iter = lis.apply(i)
                    encontrado = true
                }

                i = i + 1
            }
            pos = pos + 1
            val res = Resultado(iter.id, iter.ia, iter.valores)
            res
        }
        result.iterator
    }

    /** insertAll es una función que combina dos vecindades de una instancia. El resultado es una k vecindad de las distancias más cercanas a una instancia.
     *
     * @param a     es un arreglo de tipo Double que representa una vecindad de una instancia. Esta compuesto por las distancias de las instancias cercanas.
     * @param b     es un arreglo de tipo Double que representa una vecindad de una instancia. Esta compuesto por las distancias de las instancias cercanas.
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un arreglo de Double que representa las k distancias más cercanas de una instancia.
     */
    def insertAll(a: Array[Double], b: Array[Double], spark: SparkSession): Array[Double] = {

        var l = Array[Double]()
        if (a.length > 0 && b.length > 0) {
            l = a
            for (i <- 0 until b.length)
                l = insert(b.apply(i), l, spark)

            l
        }
        else if (a.length > 0) {
            l = a

            l
        }
        else {
            l = b

            l
        }
    }

    /** fase1 es una función que determina el indice de anomalía de una instancia en su partición. Inicialmente se obtiene las vecindades de las instancias en la partición en que se encuentran.
     * Luego a paritr de esta vecindad local se determina el índice de anomalia de la instancia.
     *
     * @param lista es un arreglo de tipo Tupla que representa una partición de los datos
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un iterador de tipo TuplaFase1 que representa la partición de los datos que recibió la función con el índice de anomalía agregado a cada instancia.
     */
    def fase1(lista: Array[Tupla], spark: SparkSession): Iterator[TuplaFase1] = {

        var cant = 0
        var tam = lista.length.toDouble
        val iter = lista.map { x =>
            var l = Array[Double]()

            l = lista.aggregate(l)((v1, v2) => insert(distanceds(x.valores, v2.valores, spark), v1, spark), (p, set) => insertAll(p, set, spark))

            cant = cant + 1
            TuplaFase1(x.id, x.valores, IA(l, spark))
        }
        iter.toIterator

    }

    /** fase1Banco es una función que determina el indice de anomalía de una instancia en su partición. Inicialmente se obtiene las vecindades de las instancias en la partición en que se encuentran.
     * Luego a paritr de esta vecindad local se determina el índice de anomalia de la instancia.
     *
     * @param lista es un arreglo de tipo TuplaBanco que representa una partición de los datos
     * @return Retorna un iterador de tipo TuplaFase1Banco que representa la partición de los datos que recibió la función con el índice de anomalía agregado a cada instancia.
     */
    def fase1Banco(lista: Array[TuplaBanco], spark: SparkSession): Iterator[TuplaFase1Banco] = {

        var size = lista.length.toDouble
        var por = 0.toDouble
        var mil = 1.toDouble
        val iter = lista.map { x =>
            var l = Array[Double]()

            l = lista.aggregate(l)((v1, v2) => insert(distancedsBanco2(x, v2, spark), v1, spark), (p, set) => insertAll(p, set, spark))
            por = por + 1.toDouble
            if ((por / 1000.toDouble) > mil) {
                mil = mil + 1
            }
            TuplaFase1Banco(x.id, x.mes, x.importe, IA(l, spark))
        }
        iter.toIterator
    }

    /** IA es una función que determina el índice de anomalía a partir de una vecindad de una instancia. El índice de anomalia no es mas que la suma de todas las distancias de los k vecinos cercanos.
     *
     * @param d     es un arreglo de tipo Double que representa las distancias de los k vecinos de una instancia a esta
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un Double que representa el índice de anomalía.
     */
    def IA(d: Array[Double], spark: SparkSession): Double = {

        //    try{
        val id = d.reduce((a, b) => a + b)
        id
        //      }
        //    catch {
        //      case e: Exception => mylogger.error(e.getMessage)
        //        RegistroTrazas.writeLog(spark,InfoLogs.path,InfoLogs.executionType,InfoLogs.executionPeriod,Level.FATAL,"KnnwoOD","IA",e.getMessage)
        //        -1 //Codigo de error
        //    }
    }

    /** insert es una función que inserta de manera ordenada en un arreglo un valor de tipo double. Esta función se emplea para determinar las k distancias más cercanas de una instancia.
     *
     * @param x     es un Double que representa una distancia
     * @param list  es un arreglo de Double que representa las distancias más cercanas de una instancia
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un arreglo de Double que representa las k distancias más cercanas de una instancia.
     */
    def insert(x: Double, list: Array[Double], spark: SparkSession): Array[Double] = {

        var tempL: Array[Double] = list
        if (!x.isNaN) {
            if (tempL.isEmpty) {
                tempL = tempL.+:(x)
                tempL
            } else if (x < tempL.last) {
                if (k > tempL.length) {
                    insert(x.toDouble, tempL.init, spark) ++ (tempL.takeRight(1))
                }
                else {
                    tempL = insert(x.toDouble, tempL.init, spark)
                    tempL
                }
            }
            else if (k > tempL.length) {
                tempL = tempL.+:(x)
                tempL
            }
            else
                tempL
        }
        else
            tempL

    }

    /** distanceds es una función que determina la distancia euclidiana entre dos filas
     *
     * @param row1  es un arreglo de tipo Double que representa una fila de la base de datos
     * @param row2  es un arreglo de tipo Double que representa una fila de la base de datos
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un Double que representa la distancia euclidiana entre las filas row1 y row2.
     */
    def distanceds(row1: Array[Double], row2: Array[Double], spark: SparkSession): Double = {

        //    try{
        var i = 0
        var sumatoria: Double = 0
        val tam = row2.length
        while (i < tam) {
            sumatoria += math.pow(row1.apply(i) - row2.apply(i), 2)
            i += 1
        }
        math.sqrt(sumatoria)
        //        }
        //    catch {
        //      case e: Exception => mylogger.error(e.getMessage)
        //        RegistroTrazas.writeLog(spark,InfoLogs.path,InfoLogs.executionType,InfoLogs.executionPeriod,Level.FATAL,"KnnwoOD","distanceds",e.getMessage)
        //        null
        //    }
    }

    /**
     *
     * @param row
     * @param iter
     * @param spark es el SparkSession de la aplicación
     * @return
     */
    def distancedsBanco(row: TuplaFase1Banco, iter: TuplaFase1Banco, spark: SparkSession): Double = {

        //   try{
        var i = 0
        var sumatoria: Double = 0
        val primerRango = 50
        val segundoRango = 2000
        val peso1 = 0.8
        val peso2 = 0.2
        sumatoria += (Math.abs(row.mes - iter.mes) * peso2)
        if (Math.abs(row.importe - iter.importe) <= primerRango)
            sumatoria += 0 * peso1
        else if (Math.abs(row.importe - iter.importe) >= segundoRango)
            sumatoria += 1 * peso1
        else
            sumatoria += 0.5 * peso1

        sumatoria
        //       }
        //   catch {
        //     case e: Exception => mylogger.error(e.getMessage)
        //       RegistroTrazas.writeLog(spark,InfoLogs.path,InfoLogs.executionType,InfoLogs.executionPeriod,Level.FATAL,"KnnwoOD","distancedsBanco",e.getMessage)
        //       null
        //   }
    }

    /**
     *
     * @param row
     * @param iter
     * @param spark es el SparkSession de la aplicación
     * @return
     */
    def distancedsBanco2(row: TuplaBanco, iter: TuplaBanco, spark: SparkSession): Double = {

        //    try{
        var i = 0
        var sumatoria: Double = 0
        val primerRango = 50
        val segundoRango = 2000
        val peso1 = 0.8
        val peso2 = 0.2
        sumatoria += (Math.abs(row.mes - iter.mes) * peso2)
        if (Math.abs(row.importe - iter.importe) <= primerRango)
            sumatoria += 0 * peso1
        else if (Math.abs(row.importe - iter.importe) >= segundoRango)
            sumatoria += 1 * peso1
        else
            sumatoria += 0.5 * peso1

        sumatoria

        //     }
        //    catch {
        //      case e: Exception => mylogger.error(e.getMessage)
        //        RegistroTrazas.writeLog(spark,InfoLogs.path,InfoLogs.executionType,InfoLogs.executionPeriod,Level.FATAL,"KnnwoOD","distancedsBanco2",e.getMessage)
        //        null
        //   }
    }

    /** parseTupla es una función que convirte el tipo de dato Row al tipo de dato Tupla
     *
     * @param row   es una fila de la base de datos del tipo Row
     * @param spark es el SparkSession de la aplicación
     * @return Retorna una objeto de tipo Tupla
     */
    def parseTupla(row: Row, spark: SparkSession, ID: String = "ID"): Tupla = {
        var valores = Array[Double]()
        val id = row.getString(row.fieldIndex(ID))
        valores = row.toSeq.filter(_.toString != id).map(_.toString.toDouble).toArray
        Tupla(id, valores)
    }


    /** parseTuplaBanco es una función que convirte el tipo de dato Row al tipo de dato Tupla
     *
     * @param row   es una fila de la base de datos del tipo Row
     * @param spark es el SparkSession de la aplicación
     * @return Retorna una objeto de tipo TuplaBanco
     */
    def parseTuplaBanco(row: Row, spark: SparkSession): TuplaBanco = {

        val id = row.getString(2).toLong
        val mes = row.getString(0).toInt
        val importe = row.getString(1).toDouble
        TuplaBanco(id, mes, importe)

    }

}
