import java.awt.{Color, Dimension}
import java.io._
import java.net.URLDecoder
import java.util.concurrent.TimeUnit
import javax.servlet.ServletException
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.github.benmanes.caffeine.cache.{Cache, CacheWriter, Caffeine, RemovalCause}
import com.supermap.bdt.mapping.{DMap, HdfsClient}
import com.supermap.data.{Point2D, Rectangle2D, Workspace, WorkspaceConnectionInfo}

import scala.compat.java8.FunctionConverters._


/**
  * Created by Administrator on 2017/8/30.
  */
abstract class  MapImageServerlet(m_dmap: DMap) extends HttpServlet{
  private val serialVersionUID: Long = 1L

  def getImage(width: Int, height: Int, scale: Double, viewBounds: Rectangle2D, z: Int, y: Int, x: Int): Array[Byte]

  @throws[ServletException]
  @throws[IOException]
  override protected def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    println(request.getQueryString())

    val args = URLDecoder.decode(request.getQueryString(),"UTF-8").split("&")
    val hashValues = new scala.collection.mutable.HashMap[String,String]()
    for( i <- 0 until args.length){
      val keyAndValue = args(i).split("=");
      hashValues.put(keyAndValue(0),keyAndValue(1))
    }

    val scale:Double = hashValues.get("scale").getOrElse("4.9014683554480507429591568955777e-7").toDouble
    val width:Int = hashValues.get("width").getOrElse("1440").toInt
    val height:Int = hashValues.get("height").getOrElse("900").toInt
//    val centerStr = hashValues.get("center").getOrElse[String]("{x:102,y:35}")
//    val centerHash = JSON.parse(centerStr).asInstanceOf[JSONObject]
//    val tile = hashValues.get("tile").getOrElse("-1").toInt
//    val filter = hashValues.get("filter").getOrElse("true").toBoolean

    val viewBoundsStr = hashValues.get("viewBounds").getOrElse("")
    var viewBounds:Rectangle2D = null
    if(viewBoundsStr.length > 0){
      val rectObj = JSON.parse(viewBoundsStr).asInstanceOf[JSONObject]
      val leftBottomObj = rectObj.get("leftBottom").asInstanceOf[JSONObject]
      val lettBottom = new Point2D(leftBottomObj.get("x").toString.toDouble,leftBottomObj.get("y").toString.toDouble)
      val rightTopObj = rectObj.get("rightTop").asInstanceOf[JSONObject]
      val rightTop = new Point2D(rightTopObj.get("x").toString.toDouble,rightTopObj.get("y").toString.toDouble)
      viewBounds = new Rectangle2D(lettBottom,rightTop)
    }

    // TODO 支持处理ViewBounds的请求情况

    val x= hashValues.get("x").getOrElse("0").toInt
    val y= hashValues.get("y").getOrElse("0").toInt
    val z= hashValues.get("z").getOrElse("0").toInt
    val start = System.currentTimeMillis()

    val bits =  this.getImage(width,height,scale,viewBounds,z, y,x)

    if(bits != null){
      response.getOutputStream.write(bits)
      response.setStatus(HttpServletResponse.SC_OK)
    }else{
      response.setStatus(HttpServletResponse.SC_SEE_OTHER)
    }
  }
}


class RDDHdfsServerlet(m_dmap: DMap) extends MapImageServerlet(m_dmap){
  override def getImage(width: Int, height: Int, scale: Double, viewBounds: Rectangle2D, z: Int, y: Int, x: Int): Array[Byte] ={

    val dimension = new Dimension(width, height)
    if(viewBounds != null && !viewBounds.isEmpty){
      //println(s"outputMapToBitmap1 size:${dimension.toString} viewBounds:${viewBounds.toString}")
      m_dmap.outputMapToBitmap(dimension, viewBounds)
    }else{
      null
    }
  }

}

class CachedServerlet(m_dmap: DMap, cache: Cache[String, Array[Byte]], cachePath : String) extends MapImageServerlet(m_dmap){
  override def getImage(width: Int, height: Int, scale: Double, viewBounds: Rectangle2D, z: Int, y: Int, x: Int): Array[Byte] ={
    // TODO　cache 文件的存取
    val key = s"${z}/${y}/${x}"
    cache.get(key, ((k : String) =>  {

      var result : Array[Byte] = null
      if(!cachePath.isEmpty){
        val filePath = cachePath + key + ".png"
        val hdfsClient = HdfsClient(filePath)

        if(hdfsClient.isExists()){
          result = hdfsClient.read()
        }
      }

      if(result == null){
        result = {
          val dimension = new Dimension(width, height)
          if(viewBounds != null && !viewBounds.isEmpty){
            m_dmap.outputMapToBitmap(dimension, viewBounds)
          }else{
            null
          }
        }
      }

      result
    }).asJava)
  }
}

//class RDDHdfsStyleSeverlet extends HttpServlet{
//  private val serialVersionUID: Long = 1L
//
//
//
//  // 浏览器的url中只能是get请求，为了测试方便，这里get 和 post为同样的功能
//  @throws[ServletException]
//  @throws[IOException]
//  override protected def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
//    this.doPost(request,response)
//  }
//
//  @throws[ServletException]
//  @throws[IOException]
//  override  protected def doPost(request: HttpServletRequest, response: HttpServletResponse):Unit = {
//    // 参数为图层名称，修改后的前景色，背景色，使用的0xRRGGBB方式的整型值
//    response.setStatus(HttpServletResponse.SC_OK)
//    val args = URLDecoder.decode(request.getQueryString(),"UTF-8").split("&")
//    val hashValues = new scala.collection.mutable.HashMap[String,String]()
//    for( i <- 0 until args.length){
//      val keyAndValue = args(i).split("=");
//      hashValues.put(keyAndValue(0),keyAndValue(1))
//    }
//
//
//    val layerName:String = hashValues.get("layer").getOrElse("1")
//
//    val fC = Integer.valueOf(hashValues.get("foreColor").getOrElse("0x0").replace("0x",""),16)
//    val bC = Integer.valueOf(hashValues.get("backColor").getOrElse("0xFFFFFF").replace("0x",""),16)
//    val itemName:String = hashValues.get("item").getOrElse("城市")
//    val foreColor:Color = new Color(fC)
//    val backColor:Color= new Color(bC)
//    println(layerName,fC,bC,itemName)
//    val status = SparkMapping.setStyle(layerName,itemName,foreColor,backColor)
//    response.setContentType("text")
//    response.getWriter.println(status)
//
//
//  }
//
//}

class StatusSeverlet extends HttpServlet {
  private val serialVersionUID: Long = 1L


  // 浏览器的url中只能是get请求，为了测试方便，这里get 和 post为同样的功能
  @throws[ServletException]
  @throws[IOException]
  override protected def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("text")
    //response.getWriter.println(SparkMapping.StartCost)
  }
}