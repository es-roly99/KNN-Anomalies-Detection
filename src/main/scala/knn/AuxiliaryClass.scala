package knn

object AuxiliaryClass {

    /** Neighorhood es una estructura que representa una vecidad de Tuplas
     *
     * @param pivot Tupla que representa el centro de la vecindad
     * @param neighbors Array de Tupla que representa los vecinos que pertenecen a la vecindad
     * @param neighborsNew Array de Tupla que representa los nuevos vecinos que pertenecen a la vecindad, se utiliza para el flujo del algorimo
     */
    case class Neighborhood(pivot: Tuple, neighbors: Seq[Tuple], neighborsNew: Seq[Tuple]) extends Serializable


    /** Tuple es la estructura inicial que se utiliza en los DataSets
     *
     * @param id String que representa el identificador único de la Tupla
     * @param values Arreglo de Double que representa los valores de los atributos de la tupla
     * @param distances Arreglo de Double que representa las distancias de  los k vecinos mas cercanos
     */
    case class Tuple(id: String, values: Array[Double], distances: Array[Double]) extends Serializable


    /** TupleStage1 es una estructura que se utiliza en el DataSet que se obtiene de la fase1
     *
     * @param id Long que representa el identificador único de la tupla
     * @param values Arreglo de Double que representa los valores de los atributos de la tupla
     * @param ia Double que representa el índice de anomalía de la tupla
     * @param distances Arreglo de Double que representa las distancias de  los k vecinos mas cercanos
     */
    case class TupleStage1(id: String, values: Seq[Double], ia: Double, distances: Seq[Double]) extends Serializable


    /** Resultado es una estructura que se utiliza en el DataSet que retorna una Tupla clasificada
     *
     * @param id Long que representa el identificador único de la tupla
     * @param ia Double que representa el índice de anomalía de la tupla
     * @param distances Arreglo de Double que representa las distancias de  los k vecinos mas cercanos
     * @param values Arreglo de Double que representa los valores de los atributos de la tupla
     * @param classification String que representa la tupla clasificada en anomala o normal
     */
    case class Result(id: String, ia: Double, distances: Seq[Double], values: Seq[Double], classification: String) extends Serializable

}
