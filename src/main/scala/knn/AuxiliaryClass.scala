package knn

object AuxiliaryClass {

    /** Tuple es una estructura que se utiliza en los DataSets
     *
     * @constructor crea una nueva Tupla con su identificador y un arreglo compuesto por los valores de los atributos de la tupla.
     * @param id      es un Long que representa el identificador único de la Tupla
     * @param values es un arreglo de Double que representa los valores de los atributos de la tupla
     */
    case class Tuple(id: String, values: Array[Double], distances: Array[Double]) extends Serializable


    /** TuplaFase1 es una estructura que se utiliza en el DataSet que se obtiene de la función fase1
     *
     * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
     * @param id      es un Long que representa el identificador único de la tupla
     * @param values es un arreglo de Double que representa los valores de los atributos de la tupla
     * @param ia      es un Double que representa el índice de anomalía de la tupla
     */
    case class TupleStage1(id: String, values: Seq[Double], ia: Double, distances: Seq[Double]) extends Serializable


    /** Resultado es una estructura que se utiliza en el DataSet que retorna la función exce
     *
     * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
     * @param id es un Long que representa el identificador único de la tupla
     * @param ia es un Double que representa el índice de anomalía de la tupla
     */
    case class Result(id: String, ia: Double, distances: Seq[Double], values: Seq[Double], classification: String) extends Serializable


    case class Neighborhood(pivot: Tuple, neighbors: Seq[Tuple], neighborsNew: Seq[Tuple]) extends Serializable
}
