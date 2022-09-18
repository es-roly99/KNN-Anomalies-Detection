package knn

object AuxiliaryClass {

    /** Tupla es una estructura que se utiliza en los DataSets
     *
     * @constructor crea una nueva Tupla con su identificador y un arreglo compuesto por los valores de los atributos de la tupla.
     * @param id      es un Long que representa el identificador único de la Tupla
     * @param values es un arreglo de Double que representa los valores de los atributos de la tupla
     */
    case class Tupla(id: String, values: Array[Double], distance: Array[Double]) extends Serializable

    /** TuplaBanco es una estructura que se utiliza en los DataSets
     *
     * @constructor crea una nueva tupla con su identificador, el dia del mes y el importe.
     * @param id      es un Long que representa el identificador único de la Tupla
     * @param mes     es un entero que representa el dia del mes
     * @param importe es un Double que representa el importe
     */
    case class TuplaBanco(id: Long, mes: Int, importe: Double) extends Serializable

    /** TuplaFase1 es una estructura que se utiliza en el DataSet que se obtiene de la función fase1
     *
     * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
     * @param id      es un Long que representa el identificador único de la tupla
     * @param valores es un arreglo de Double que representa los valores de los atributos de la tupla
     * @param ia      es un Double que representa el índice de anomalía de la tupla
     */
    case class TuplaFase1(id: String, valores: Seq[Double], ia: Double, distance: Seq[Double]) extends Serializable

    /** TuplaFase1Banco es una estructura que se utiliza en el DataSet que se obtiene de la función fase1Banco
     *
     * @constructor crea una nueva tupla con su identificador único, el dia del mes, el importe y el  índice de anomalías.
     * @param id      es un Long que representa el identificador único de la tupla
     * @param mes     es un entero que representa el dia del mes
     * @param importe es un Double que representa el importe
     * @param ia      es un Double que representa el índice de anomalía de la tupla
     */
    case class TuplaFase1Banco(id: Long, mes: Int, importe: Double, ia: Double) extends Serializable

    /** TuplaFase2 es una estructura que se utiliza en el DataSet en la función fase2
     *
     * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
     * @param id         es un Long que representa el identificador único de la tupla
     * @param valores    es un arreglo de Double que representa los valores de los atributos de la tupla
     * @param distancias es un arreglo de Double que representa las distancias de los k vecinos cercanos de la tupla
     */
    case class TuplaFase2(id: String, valores: Seq[Double], distancias: Seq[Double]) extends Serializable

    /** TuplaFase2 es una estructura que se utiliza en el DataSet en la función fase2Banco
     *
     * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
     * @param id         es un Long que representa el identificador único de la tupla
     * @param mes        es un entero que representa el dia del mes
     * @param importe    es un Double que representa el importe
     * @param distancias es un arreglo de Double que representa las distancias de los k vecinos cercanos de la tupla
     */
    case class TuplaFase2Banco(id: Long, mes: Int, importe: Double, distancias: Seq[Double]) extends Serializable

    /** Resultado es una estructura que se utiliza en el DataSet que retorna la función exce
     *
     * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
     * @param ID es un Long que representa el identificador único de la tupla
     * @param ia es un Double que representa el índice de anomalía de la tupla
     */
    case class Resultado(ID: String, ia: Double, data: Seq[Double], distance: Seq[Double]) extends Serializable
    case class Clasificacion(id: String, ia: Double, distance: Seq[Double], data: Seq[Double], tipo: String) extends Serializable

    case class Neighborhood(pivot: Tupla, neighbors: Seq[Tupla], neighborsNew: Seq[Tupla]) extends Serializable
    case class ClosetNeighbors(pivot: Tupla, neighbors: Seq[Tupla]) extends Serializable

    /** ResultadoBanco es una estructura que se utiliza en el DataSet que retorna la función exce
     *
     * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
     * @param id      es un Long que representa el identificador único de la tupla
     * @param mes     es un entero que representa el dia del mes
     * @param importe es un Double que representa el importe
     * @param ia      es un Double que representa el índice de anomalía de la tupla
     */
    case class ResultadoBanco(id: Long, mes: Int, importe: Double, ia: Double) extends Serializable


}
