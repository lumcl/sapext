class MonthEnd

  def self.insert_ke30x(vtweg, perio)
    sql = "select count(*) cnt from tmplum.ke30x where perio='#{perio}' and vtweg='#{vtweg}' and rldat <> ' '"
    return if Db.find_by_sql(sql).first.cnt > 0

    sql = "delete from tmplum.ke30x where perio='#{perio}' and vtweg='#{vtweg}'"
    Db.connection.execute(sql)

    sql = "
      insert into tmplum.ke30x
        select a.vtweg,a.perio,a.budat,a.kndnr,a.artnr,a.kaufn,a.kdpos,b.vgbel,b.vgpos,b.vbeln,b.posnr,b.charg,
              decode(b.shkzg,'X',b.fkimg*-1,b.fkimg)fkimg,b.vrkme,a.vvr10,
              b.arktx,c.budat mfgdt,c.aufnr,' ' rldat,a.rowid rid,a.paobjnr,a.pasubnr,a.belnr,a.gjahr
          from sapsr3.ce13058 a
            join sapsr3.vbrp b on b.mandt=a.mandt and b.vbeln=a.rbeln and b.posnr=a.rposn
            left join tmplum.mch1x c on c.matnr=a.artnr and c.charg=b.charg
          where a.mandt='168' and a.paledger='01' and a.vrgar='F' and a.versi=' ' and a.perio='#{perio}'
          and a.vtweg='#{vtweg}'
    "
    Db.connection.execute(sql)

    sql = "
      insert into tmplum.aufkx(aufnr,stat,chgnr,bomlv,xstat,xdate,xtime,remark)
        select a.aufnr,b.stat,b.chgnr,0 bomlv,' ' xstat, ' ' xdate, ' ' xtime, ' ' remark
          from sapsr3.aufk a
            left join sapsr3.jest b on b.mandt=a.mandt and b.objnr=a.objnr and b.inact=' ' and b.stat in ('I0045','I0046')
            left join tmplum.aufkx d on d.aufnr=a.aufnr
          where a.mandt='168' and a.aufnr in (select aufnr from ke30x where rldat=' ' and perio='#{perio}' and vtweg='#{vtweg}') and d.aufnr is null
    "
    Db.connection.execute(sql)
  end


end