class Db < ActiveRecord::Base
  self.table_name = :dual
  self.primary_key = :dummy

  def self.insert_mch1x
    Db.connection.execute "
      insert into tmplum.mch1x
      select a.matnr,a.charg,a.mjahr,a.mblnr,a.zeile,a.budat,a.bwart,a.aufnr,a.ebeln,a.ebelp,a.lifnr,a.cpudt
        from (
          select a.mandt,a.matnr,a.charg,a.mjahr,a.mblnr,a.zeile,a.bwart,a.aufnr,a.ebeln,a.ebelp,a.lifnr,b.budat,b.cpudt,
             rank() over (partition by a.matnr,a.charg order by b.cpudt,b.cputm,a.mjahr,a.mblnr,a.zeile) seq
          from sapsr3.mseg a
            join sapsr3.mkpf b on b.mandt=a.mandt and b.mjahr=a.mjahr and b.mblnr=a.mblnr and b.cpudt >= (select max(cpudt) from mch1x)
            left join tmplum.mch1x c on c.matnr=a.matnr and c.charg=a.charg
        where a.mandt='168' and a.shkzg='S' and c.matnr is null) a
      where seq = 1
   "
  end

end