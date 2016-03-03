package eu.pepot.eu.spark.inputsplitter.common.serializing

import java.io.{ObjectInputStream, FileInputStream}

class Serializer(fileInputStream: FileInputStream) extends ObjectInputStream(fileInputStream) {

  override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
    try {
      Class.forName(desc.getName, false, getClass.getClassLoader)
    } catch {
      case ex: ClassNotFoundException => super.resolveClass(desc)
    }
  }

}

