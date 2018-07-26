
import com.supermap.bdt.mapping.DMap
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder

object Startup {
  def main(args: Array[String]): Unit = {

    val dt = "0x0D0D0D"

    val fC = Integer.valueOf(dt.replace("0x", ""), 16);

    val start = System.currentTimeMillis()
    val server = new Server(8010);
    //println(com.sun.media.imageioimpl.common.PackageUtil.getVendor())

    val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context)

    //context.addServlet(new ServletHolder(new LocalMapImageServerlet()), "/localimage");
    //context.addServlet(new ServletHolder(new RDDMapImageServerlet()), "/rddimage");
    //context.addServlet(new ServletHolder(new RDDMapImageServerlet()), "/rdd/image.png");
    //context.addServlet(new ServletHolder(new RDDHdfsStyleSeverlet()), "/style.set");
    //context.addServlet(new ServletHolder(new StatusSeverlet()), "/status");
    if (args.length > 1) {
      val dMap = new DMap()
      dMap.initialize(args(0), args(1))

      // 创建数据所需要的ls
      val dStart = System.currentTimeMillis()
      println("start buildhdfsIndex")

      var savePyrimid = true
      if(args.length > 3){
        savePyrimid = args(3).toBoolean
      }

      dMap.loadData(args(2), savePyrimid)

      println("end buildhdfsIndex.(ms)", System.currentTimeMillis() - dStart)
      context.addServlet(new ServletHolder(new RDDHdfsServerlet(dMap)), "/image.png");
    }

    println("start cost(ms):", System.currentTimeMillis() - start)
    server.start();
    server.join();
  }
}