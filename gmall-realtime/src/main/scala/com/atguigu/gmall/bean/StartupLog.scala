package com.atguigu.gmall.bean

import java.text.SimpleDateFormat
import java.util.Date

case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long,
                      var logDate: String = null, // 2020-05-29
                      var logHour: String = null) { // 01   02   03 ...
                        val d = new Date(ts)
                        logDate = new SimpleDateFormat("yyyy-MM-dd").format(d)
                        logHour = new SimpleDateFormat("HH").format(d)
                      }
