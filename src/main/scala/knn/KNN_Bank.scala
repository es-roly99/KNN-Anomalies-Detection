package knn

import knn.KNN.{IA, insert, insertAll}
import knn.AuxiliaryClass.{TuplaBanco, TuplaFase1Banco, TuplaFase2Banco}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object KNN_Bank {


    /** fase1Banco es una función que determina el indice de anomalía de una instancia en su partición. Inicialmente se obtiene las vecindades de las instancias en la partición en que se encuentran.
     * Luego a paritr de esta vecindad local se determina el índice de anomalia de la instancia.
     *
     * @param lista es un arreglo de tipo TuplaBanco que representa una partición de los datos
     * @return Retorna un iterador de tipo TuplaFase1Banco que representa la partición de los datos que recibió la función con el índice de anomalía agregado a cada instancia.
     */
    def fase1Banco(lista: Array[TuplaBanco], k: Int, spark: SparkSession): Iterator[TuplaFase1Banco] = {

        var por = 0.toDouble
        var mil = 1.toDouble
        val iter = lista.map { x =>
            var l = Array[Double]()

            l = lista.aggregate(l)((v1, v2) => insert(distancedsBanco2(x, v2, spark), v1, k, spark), (p, set) => insertAll(p, set, k, spark))
            por = por + 1.toDouble
            if ((por / 1000.toDouble) > mil) {
                mil = mil + 1
            }
            TuplaFase1Banco(x.id, x.mes, x.importe, IA(l, spark))
        }
        iter.toIterator
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
    def fase2Banco(lista: Broadcast[Array[(TuplaFase1Banco)]], rdd: Dataset[TuplaFase1Banco], k:Int, spark: SparkSession): Dataset[TuplaFase1Banco] = {

        import spark.implicits._

        val result = rdd.mapPartitions { iterator =>
            val arr = iterator.toArray
            val size = lista.value.length
            var por = 0.toDouble
            var mil = 1.toDouble
            val r = lista.value.map { x =>

                val l = Array[Double]()
                val iter = arr.aggregate(l)((v1, v2) => insert(distancedsBanco(x, v2, spark), v1, k, spark), (p, set) => insertAll(p, set, k, spark))
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
        val reduce = result.groupByKey(_.id).reduceGroups((a, b) => TuplaFase2Banco(a.id, a.mes, a.importe, insertAll(a.distancias.toArray, b.distancias.toArray, k, spark).toSeq))
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
    def updateBanco(lista: Broadcast[Array[TuplaFase1Banco]], rdd: Array[TuplaFase1Banco], spark: SparkSession): Iterator[TuplaFase1Banco] = {

        var pos = 0
        val result = rdd.map { iterator =>
            var r = iterator
            val lis = lista.value
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


    /**
     *
     * @param row
     * @param iter
     * @param spark es el SparkSession de la aplicación
     * @return
     */
    def distancedsBanco2(row: TuplaBanco, iter: TuplaBanco, spark: SparkSession): Double = {

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
    }


    /**
     *
     * @param row
     * @param iter
     * @param spark es el SparkSession de la aplicación
     * @return
     */
    def distancedsBanco(row: TuplaFase1Banco, iter: TuplaFase1Banco, spark: SparkSession): Double = {

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
    }
}
