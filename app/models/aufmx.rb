class Aufmx < Db
  self.primary_key = :uuid
  self.table_name  = :aufmx

  def insert(aufnr)
    sql = "delete from tmplum.aufmx where aufnr='#{aufnr}'"
    Db.connection.execute(sql)

    # wip material
    sql           = "
      select a.aufnr,a.rsnum,a.rspos,a.mjahr,a.mblnr,a.zeile,a.budat,
             a.bwart,a.matnr,a.charg,a.werks,decode(a.shkzg,'H',a.menge,a.menge*-1)menge,
             a.meins,decode(a.shkzg,'H',a.dmbtr,a.dmbtr*-1)dmbtr,
             nvl(b.aufnr,' ') waufnr, decode(a.shkzg,'H',a.menge,a.menge*-1) qty,
             decode(a.shkzg,'H',a.dmbtr,a.dmbtr*-1) amt, 'M' wip,
             to_char(rawtohex(sys_guid())) uuid,
             c.xstat,d.matnr pmatnr,d.wemng wmoqty, 0 wfactor
        from sapsr3.aufm a
          join tmplum.mch1x b on b.matnr=a.matnr and b.charg=a.charg and b.aufnr <> ' '
          left join tmplum.aufkx c on c.aufnr=b.aufnr
          left join sapsr3.afpo d on d.mandt='168' and d.aufnr=b.aufnr
        where a.mandt='168' and a.rspos <> '0000' and a.aufnr=?
    "
    rows          = Db.find_by_sql([sql, aufnr])
    # 檢查小MO是否已經展開, 如果沒有立刻停止
    not_ready_mos = []
    rows.each do |row|
      if row.waufnr.eql?('ZIEBP023')
        row.wmoqty  = 1
        row.wfactor = row.menge
      else
        unless row.xstat.eql?('X')
          not_ready_mos.append(row.waufnr) unless not_ready_mos.include?(row.waufnr)
        end
        # 計算入庫數量, 沒有在resb posr的都認為是產出
        row.wmoqty  = get_wip_order_qty(row) #unless row.matnr.eql?(row.pmatnr)
        row.wfactor = row.menge / row.wmoqty
      end
    end

    return not_ready_mos.join(',') if not_ready_mos.present?

    rows.each do |row|
      Aufmx.create(uuid:    row.uuid,
                   aufnr:   row.aufnr,
                   wip:     'W',
                   rsnum:   row.rsnum,
                   rspos:   row.rspos,
                   mblnr:   row.mblnr,
                   mjahr:   row.mjahr,
                   zeile:   row.zeile,
                   budat:   row.budat,
                   bwart:   row.bwart,
                   matnr:   row.matnr,
                   charg:   row.charg,
                   werks:   row.werks,
                   menge:   row.menge,
                   qty:     0,
                   meins:   row.meins,
                   dmbtr:   row.dmbtr,
                   amt:     0,
                   waufnr:  row.waufnr,
                   wmoqty:  row.wmoqty,
                   wfactor: row.wfactor
      )

      if row.waufnr.eql?('ZIEBP023')
        read_ziebp023(row)
      else
        read_aufmx(row)
      end
    end


    # pure material direct insert to tmplum.aufmx
    sql = "
      insert into tmplum.aufmx(aufnr,rsnum,rspos,mjahr,mblnr,zeile,budat,bwart,matnr,charg,werks,menge,meins,dmbtr,waufnr,qty,amt,wip)
        select * from (
          select a.aufnr,a.rsnum,a.rspos,a.mjahr,a.mblnr,a.zeile,a.budat,
                 a.bwart,a.matnr,a.charg,a.werks,decode(a.shkzg,'H',a.menge,a.menge*-1)menge,
                 a.meins,decode(a.shkzg,'H',a.dmbtr,a.dmbtr*-1)dmbtr,
                 nvl(b.aufnr,' ') waufnr, decode(a.shkzg,'H',a.menge,a.menge*-1) qty,
                 decode(a.shkzg,'H',a.dmbtr,a.dmbtr*-1) amt, 'M' wip
            from sapsr3.aufm a
              left join tmplum.mch1x b on b.matnr=a.matnr and b.charg=a.charg
            where a.mandt='168' and a.rspos <> '0000' and a.aufnr='#{aufnr}')
          where waufnr=' '
    "
    Db.connection.execute(sql)

    return 'OK'
  end

  def read_ziebp023(row)
    sql     = "
      select idnrk,menge,to_char(rawtohex(sys_guid())) uuid from sapsr3.ziebp023
        where mandt='168' and bnarea='2300' and bukrs='L400' and connr='E23070000001'
          and matnr=? and charg=?
    "
    records = Db.find_by_sql([sql, row.matnr, row.charg])
    records.each do |record|
      Aufmx.create(
          uuid:    row.uuid,
          aufnr:   row.aufnr,
          wip:     'S',
          rsnum:   row.rsnum,
          rspos:   row.rspos,
          mblnr:   row.mblnr,
          mjahr:   row.mjahr,
          zeile:   row.zeile,
          budat:   row.budat,
          bwart:   row.bwart,
          matnr:   record.idnrk,
          charg:   ' ',
          werks:   '481A',
          menge:   record.menge * row.wfactor,
          qty:     record.menge * row.wfactor,
          meins:   row.meins,
          dmbtr:   0,
          amt:     0,
          waufnr:  row.waufnr,
          wmoqty:  row.wmoqty,
          wfactor: row.wfactor
      )
    end
  end

  def read_aufmx(row)
    sql     = '
      select a.wip,a.aufnr,a.rsnum,a.rspos,a.mblnr,a.mjahr,a.zeile,a.budat,a.bwart,
             a.matnr,a.charg,a.werks,a.menge,a.qty,a.meins,a.dmbtr,a.amt,
             to_char(rawtohex(sys_guid())) uuid
        from tmplum.aufmx a
        where a.aufnr=?
    '
    records = Db.find_by_sql([sql, row.waufnr])
    records.each do |record|
      Aufmx.create(
          uuid:    row.uuid,
          aufnr:   row.aufnr,
          wip:     'S',
          rsnum:   row.rsnum,
          rspos:   row.rspos,
          mblnr:   row.mblnr,
          mjahr:   row.mjahr,
          zeile:   row.zeile,
          budat:   record.budat,
          bwart:   record.bwart,
          matnr:   record.matnr,
          charg:   record.charg,
          werks:   record.werks,
          menge:   record.menge,
          qty:     record.menge * row.wfactor,
          meins:   record.meins,
          dmbtr:   record.dmbtr,
          amt:     record.dmbtr * row.wfactor,
          waufnr:  record.aufnr,
          wrsnum:  record.rsnum,
          wrspos:  record.rspos,
          wmjahr:  record.mjahr,
          wmblnr:  record.mblnr,
          wzeile:  record.zeile,
          wfactor: row.wfactor,
          wmoqty:  row.wmoqty
      )
    end
  end

  def get_wip_order_qty(row)
    sql = "
      select sum(decode(a.shkzg,'S',a.menge,a.menge*-1))wmoqty
        from sapsr3.aufm a
        where a.mandt='168' and a.aufnr=? and rspos = '0000'
    "
    Db.find_by_sql([sql, row.aufnr]).first.wmoqty
  end
end