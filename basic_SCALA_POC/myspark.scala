object myspark {
def main(arge: Array[String]) {
val conf = new SparkConf().setAppName(appName).setMaster(master)
new SparkContext(conf)
}

}
