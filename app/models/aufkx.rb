class Aufkx < ActiveRecord::Base
  self.table_name  = :aufkx
  self.primary_key = :aufnr

  def self.explosion
    # update_bomlv
    rows = Aufkx.where(xstat: 'B').order(bomlv: :desc)
    rows.each do |row|
      row.xstat = 'R'
      row.save
      aufmx = Aufmx.new
      aufmx.insert(row.aufnr)
      row.xstat = 'X'
      row.xdate = Time.now.strftime('%Y%m%d')
      row.xtime = Time.now.strftime('%H%M%S')
      row.save
    end
  end

  def self.update_bomlv
    sql = "update tmplum.aufkx set xstat = ' ', bomlv = 0 where xstat <> 'X'"
    Db.connection.execute(sql)

    aufnrs = []
    sql    = "select aufnr from tmplum.aufkx where xstat = ' '"
    Db.find_by_sql(sql).each do |row|
      aufnrs.append row.aufnr
    end

    bomlv = 0
    while aufnrs.present?
      bomlv  += 1
      aufnrs = set_submo(aufnrs, bomlv)
    end
  end

  def self.set_submo(aufnrs, bomlv)
    aufkx  = {}
    submos = []
    min    = 0
    while min < aufnrs.size do
      max     = (min + 1000) < aufnrs.size ? min + 1000 : aufnrs.size
      sql     = "
        select distinct a.aufnr,b.stat,b.chgnr,c.aufnr submo
          from sapsr3.aufm a
            join sapsr3.jest b on b.mandt='168' and b.objnr='OR'||a.aufnr and b.inact=' ' and b.stat in ('I0045','I0046')
            left join tmplum.mch1x c on c.matnr=a.matnr and c.charg=a.charg
          where a.mandt='168' and a.rspos > 0 and a.aufnr in (?)
      "
      records = Db.find_by_sql [sql, aufnrs[min..(max - 1)]]
      records.each do |row|
        aufkx[row.aufnr] = { stat: row.stat, chgnr: row.chgnr, bomlv: bomlv, xstat: 'B' }
        if row.submo.present?
          submos.append row.submo unless submos.include?(row.submo)
        end
      end
      min = max
    end
    aufnrs.clear
    Aufkx.update(aufkx.keys, aufkx.values)

    submos.each do |submo|
      begin
        Aufkx.create(aufnr: submo, stat: ' ', chgnr: ' ', bomlv: 0, xstat: ' ', xdate: ' ', xtime: ' ')
      rescue
      end
    end
    return submos
  end

end