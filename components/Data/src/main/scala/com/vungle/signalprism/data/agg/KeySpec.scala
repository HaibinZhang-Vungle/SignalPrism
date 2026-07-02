package com.vungle.signalprism.data.agg

import com.vungle.lena.BoilerplateSparkMain
import org.json4s.{JValue, JObject, JArray, JString, JNull, JNothing}
import org.json4s.native.JsonMethods.parse

/** Single source of truth for spec-driven aggregation dimension + surrogate-key SQL.
 *  Used by realtime_attributed_aggregation and gminor_attributed_join so keys can't drift. */
object KeySpec {
  private def readResource(res: String): String =
    scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(res), "UTF-8").mkString
  private def parseObj(res: String): Map[String, JValue] =
    parse(readResource(res)).asInstanceOf[JObject].obj.toMap
  private def arr(a: JValue): List[JValue] = a.asInstanceOf[JArray].arr
  private def obj(a: JValue): Map[String, JValue] = a.asInstanceOf[JObject].obj.toMap
  private def str(a: JValue): String = a match {
    case JString(s) => s; case JNull | JNothing => null; case other => other.values.toString
  }

  private def spec(family: String) = parseObj(s"agg_specs/$family.json")
  def dims(family: String): List[Map[String, JValue]] = arr(spec(family)("dimensions")).map(obj)
  def primaryKeyName(family: String): String = str(obj(spec(family)("primary_key"))("name"))
  def dropNullSource(family: String): String = str(spec(family).getOrElse("drop_null_source", JNull))
  def dimNames(family: String): Seq[String] = dims(family).map(d => str(d("name")))

  private def dimExpr(d: Map[String, JValue]): String = {
    val name = str(d("name")); val src = str(d("source_col")); val norm = str(d("norm"))
    val e = norm match {
      case _ if src == null       => "CAST(NULL AS STRING)"
      case "parse_major"          => s"split($src, '\\\\.')[0]"
      case "coalesce"             => s"coalesce($src, ${str(d("fallback_col"))})"
      case "normalize" | "bucket" => s"lower(trim($src))"
      case "expr"                 => src
      case _                      => src
    }
    s"$e AS $name"
  }
  private def keyConcatArg(d: Map[String, JValue]): String =
    s"coalesce(CAST(${str(d("name"))} AS STRING), '__unknown__')"

  def dimProjections(family: String): Seq[String] = dims(family).map(dimExpr)

  def allDimProjections(families: Seq[String]): Seq[String] = {
    val byName = scala.collection.mutable.LinkedHashMap[String, String]()
    for (f <- families; d <- dims(f)) {
      val name = str(d("name")); val proj = dimExpr(d)
      byName.get(name).foreach(p => require(p == proj, s"dim '$name' has conflicting exprs across families"))
      byName(name) = proj
    }
    byName.values.toSeq
  }

  def surrogateExpr(family: String): String =
    s"sha2(concat_ws('|', ${dims(family).map(keyConcatArg).mkString(", ")}), 256)"
}
