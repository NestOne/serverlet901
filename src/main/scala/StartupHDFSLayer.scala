
import java.io.File
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{Cache, CacheWriter, Caffeine, RemovalCause}
import com.supermap.bdt.io.indexfile.IndexFileReader
import com.supermap.bdt.mapping.{DMap, HdfsClient}
import com.supermap.mapping.LayerSettingVector
import org.apache.commons.io.FileUtils
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import scala.xml.XML

object StartupHDFSLayer {
  def main(args: Array[String]): Unit = {

    //context.addServlet(new ServletHolder(new LocalMapImageServerlet()), "/localimage");
    //context.addServlet(new ServletHolder(new RDDMapImageServerlet()), "/rddimage");
    //context.addServlet(new ServletHolder(new RDDMapImageServerlet()), "/rdd/image.png");
    //context.addServlet(new ServletHolder(new RDDHdfsStyleSeverlet()), "/style.set");
    //context.addServlet(new ServletHolder(new StatusSeverlet()), "/status");
    if (args.length == 0) {
      println("need arguments!")
      println("need arg 'inputHDFS', input rdd save path.like -inputHDFS=hdfs://master:9000/test")
      println("need arg 'pyramidHDFS', save pyramid rdd path. like -pyramidHDFS=hdfs://master:9000/test")
      println("optional arg 'cachePath', save png cache path.like -cachePath=hdfs://master:9000/test")
      println("optional arg 'layerXML',layer template path, define setting or theme.like -layerXML=hdfs://master:9000/test.xml")
      println("optional arg 'memCacheSize', size of cache image in memory. like -memCacheSize=9000")
      return
    }

    val arguments = Arguments(args)
    //    val inputHDFS =  "file:///F:/Shanxi_2000/Cache/vectorpyramid/shanxi_2000_shanxi_roadR_28w/16384"
    //    val pyramidHDFS =  "file:///F:/Shanxi_2000/Cache/vectorpyramid/shanxi_2000_shanxi_roadR_28w/16384/Cache"
    val inputHDFS = arguments.get("inputHDFS").getOrElse({
      println("need arg 'inputHDFS' like -inputHDFS=hdfs://master:9000/test")
      return
    })

    val pyramidHDFS = arguments.get("pyramidHDFS").getOrElse({
      println("need arg 'pyramidHDFS' like -pyramidHDFS=hdfs://master:9000/test")
      return
    })

    val (layerSetting, theme) = arguments.get("layerXML") match {
      case Some(path) => {
        val xml = FileUtils.readFileToString(new File(path))
        val xmlElem = XML.loadString(xml)
        val (setting, t, _) = DMap.readLayerXML(xmlElem)
        (setting, t)
      }
      case _ => (new LayerSettingVector(), null)
    }

    val workspacePath = arguments.get("workspacePath") match {
      case Some(path) => path
      case None => ""
    }

    val cachePath = arguments.get("cachePath") match {
      case Some(p) => {
        val temp = p.replace("\\", "/")
        if(!temp.endsWith("/")) temp + "/" else temp
      }
      case _ => ""
    }

    val memCacheSize = arguments.get("memCacheSize").getOrElse("3000").toInt
    val port = arguments.get("port").getOrElse("8010").toInt

    val dStart = System.currentTimeMillis()

    val start = System.currentTimeMillis()
    val server = new Server(port)
    //println(com.sun.media.imageioimpl.common.PackageUtil.getVendor())

    val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/")
    server.setHandler(context)

    val dMap = new DMap()
    if(workspacePath.isEmpty){
      dMap.initialize()
    } else {
      dMap.initialize(workspacePath, "")
    }

    val inputRDD = IndexFileReader.read(dMap.m_sparkContext, inputHDFS)
    dMap.addPyramidRDDLayer(inputRDD, 0, "HDFSLayer", layerSetting, theme, true, true, pyramidHDFS)

    val cache: Cache[String, Array[Byte]] = Caffeine.newBuilder()
      .expireAfterWrite(30, TimeUnit.MINUTES)
      .maximumSize(memCacheSize)
      .writer(new CacheWriter[String, Array[Byte]] {
        override def delete(key: String, value: Array[Byte], cause: RemovalCause) = {
          if(!cachePath.isEmpty){
            val filePath = cachePath + key + ".png"
            HdfsClient(filePath).writeFile(value)
          }
        }

        override def write(key: String, value: Array[Byte]) = {

        }
      })
      .build[String, Array[Byte]]()

    // 创建数据所需要的ls
    println("start buildhdfsIndex")

    //SparkMapping.loadData(SparkMapping.WorkspaceFile, SparkMapping.MapName, args(2), indexType,savePyrimid)
    println("end buildhdfsIndex.(ms)", System.currentTimeMillis() - dStart)
    context.addServlet(new ServletHolder(new CachedServerlet(dMap, cache, cachePath)), "/tileImage.png");
    context.addServlet(new ServletHolder(new RDDHdfsServerlet(dMap)), "/image.png");

    println("start cost(ms):", System.currentTimeMillis() - start)
    server.start();
    server.join();
  }
}