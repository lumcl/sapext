class Aufmx < Db
  self.primary_key = :id
  self.table_name  = :aufmx

  def insert(aufnr)
    sql = "delete from tmplum.aufmx where aufnr='#{aufnr}'"
    Db.connection.execute(sql)

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

    # wip material
    sql  = "
      select a.aufnr,a.rsnum,a.rspos,a.mjahr,a.mblnr,a.zeile,a.budat,
             a.bwart,a.matnr,a.charg,a.werks,decode(a.shkzg,'H',a.menge,a.menge*-1)menge,
             a.meins,decode(a.shkzg,'H',a.dmbtr,a.dmbtr*-1)dmbtr,
             nvl(b.aufnr,' ') waufnr, decode(a.shkzg,'H',a.menge,a.menge*-1) qty,
             decode(a.shkzg,'H',a.dmbtr,a.dmbtr*-1) amt, 'M' wip
        from sapsr3.aufm a
          join tmplum.mch1x b on b.matnr=a.matnr and b.charg=a.charg and b.aufnr <> ' '
        where a.mandt='168' and a.rspos <> '0000' and a.aufnr=?)
    "
    rows = Db.find_by_sql([sql, aufnr])
    rows.each do |row|
      Aufmx.create(aufnr:  row.aufnr,
                   wip:    'W',
                   rsnum:  row.rsnum,
                   rspos:  row.rspos,
                   mblnr:  row.mblnr,
                   mjahr:  row.mjahr,
                   zeile:  row.zeile,
                   budat:  row.budat,
                   bwart:  row.bwart,
                   matnr:  row.matnr,
                   charg:  row.charg,
                   werks:  row.werks,
                   menge:  row.menge,
                   qty:    0,
                   meins:  row.meins,
                   dmbtr:  row.dmbtr,
                   amt:    0,
                   waufnr: row.waufnr)

      if row.waufnr.eql?('ZIEBP023')
        read_ziebp023(row)
      else
        read_aufmx(row)
      end
    end
  end

  def read_ziebp023(row)
    sql     = "
      select idnrk,menge from sapsr3.ziebp023
        where mandt='168' and bnarea='2300' and bukrs='L400' and connr='E23070000001'
          and matnr=? and charg=?
    "
    records = Db.find_by_sql([sql, row.matnr, row.charg])
    records.each do |record|
      Aufmx.create(
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
          menge:   row.menge * record.menge,
          qty:     row.menge * record.menge,
          meins:   row.meins,
          dmbtr:   0,
          amt:     0,
          waufnr:  row.waufnr,
          wfactor: 1
      )
    end
  end

  def read_aufmx(row)
    sql     = "
      select b.wemng,a.wip,a.rsnum,a.rspos,a.mblnr,a.mjahr,a.zeile,a.budat,a.bwart,
             b.matnr,a.charg,a.werks,a.menge,a.qty,a.meins,a.dmbtr,a.amt
        from tmplum.aufmx a
        join sapsr3.afpo b on b.mandt='168' and b.aufnr=a.aufnr
        where a.aufnr=?
    "
    records = Db.find_by_sql([sql, row.waufnr])
    records.each do |record|
      Aufmx.create(
          aufnr: row.aufnr,
          wip: 'S',
          rsnum: row.rsnum,
          rspos: row.rspos,
          mblnr: row.mblnr,
          mjahr: row.mjahr,
          zeile: row.zeile,
          budat: row.budat,
          bwart: row.bwart,
          matnr: record.matnr,
          charg: record.charg,
          werks: record.werks,
          menge: record.menge,
          qty: (row.menge * record.menge) / record.wemng,
          meins: row.meins,
          dmbtr: record.dmbtr,
          amt: (row.menge * record.dmbtr) / record.wemng,
          waufnr: record.aufnr,
          wrsnum: record.rsnum,
          wrspos: record.rspos,
          wmjahr: record.mjahr,
          wmblnr: record.mblnr,
          wzeile: record.zeile,
          wfactor: row.menge / record.wemng
      )
    end
  end

end