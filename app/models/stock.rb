class Stock

  def self.allocation
    Db.connection.execute('truncate table stk_mchb')
    Db.connection.execute('truncate table stk_mrp')
    Db.connection.execute('truncate table stk_aloc')

    sto = {}
    sto['101A'] = %w[101A]
    sto['111A'] = %w[111A 282A]
    sto['112A'] = %w[112A]
    sto['281A'] = %w[281A]
    sto['282A'] = %w[282A]
    sto['381A'] = %w[381A 701A 101A.RM03 101A.RM05 101A.RM06 921A]
    sto['382A'] = %w[382A]
    sto['481A'] = %w[481A 101A.RM01]
    sto['482A'] = %w[482A 101A.RM02]
    sto['921A'] = %w[921A]
    sto['701A'] = %w[701A]

    stock_hash = {}

    # sql = "
    #  select  a.matnr,a.werks,a.charg,a.clabs,a.cumlm,a.cinsm,a.ceinm,a.cspem,a.cretm,
    #          nvl(b.budat,to_char(sysdate,'YYYYMMDD')) budat,
    #          (a.clabs+a.cumlm+a.cinsm+a.ceinm+a.cspem+a.cretm) bal_qty, 0 alc_qty,lgort,
    #          case
    #            when werks='101A' and lgort in ('RM01','RM02','RM03','RM06') then lgort else '****'
    #          end sto_lgort, rawtohex(sys_guid()) uuid
    #     from sapsr3.mchb a
    #       left join tmplum.mch1x b on b.matnr=a.matnr and b.charg=a.charg
    #   where a.mandt='168' and (a.clabs+a.cumlm+a.cinsm+a.ceinm+a.cspem+a.cretm) > 0
    # "

    sql = "
      select matnr,werks,charg,lgort,budat,bal_qty,alc_qty,uuid,
             clabs,cumlm,cinsm,ceinm,cspem,cretm,lbkum,salk3,
             case
               when werks='101A' and lgort in ('RM01','RM02','RM03','RM06') then lgort else '****'
             end sto_lgort
        from tmplum.mchbx
    "
    Db.find_by_sql(sql).each do |row|
      array = stock_hash.key?("#{row.matnr}.#{row.werks}.#{row.sto_lgort}") ? stock_hash["#{row.matnr}.#{row.werks}.#{row.sto_lgort}"] : []
      array.append(row)
      stock_hash["#{row.matnr}.#{row.werks}.#{row.sto_lgort}"] = array
    end

    stk_alocs = []
    sql = "
      select werks,matnr,delkz,delnr,del12,delps,delet,dat00,
             mng01,mng01 bal_qty,0 alc_qty,rawtohex(sys_guid()) uuid, kunnr
        from sapsr3.zsd0012
      where mandt='168' and mng01 > 0 and plumi='-' and delkz not in ('U1','U2','U3')
    "
    reqs = Db.find_by_sql(sql)
    reqs.group_by(&:matnr).each do |matnr, werks|
      werks.group_by(&:werks).each do |werks, rows|
        rows.each do |req|
          puts "#{matnr},#{werks}"
          sto["#{werks}"].each do |supply_werks|
            stocks = supply_werks.size == 4 ? stock_hash["#{matnr}.#{supply_werks}.****"] : stock_hash["#{matnr}.#{supply_werks[0..3]}.#{supply_werks[5..8]}"]
            if stocks.present?
              stocks.each do |stock|
                if stock.bal_qty > 0
                  ws_alc_qty = stock.bal_qty > req.bal_qty ? req.bal_qty : stock.bal_qty
                  req.bal_qty -= ws_alc_qty
                  req.alc_qty += ws_alc_qty
                  stock.bal_qty -= ws_alc_qty
                  stock.alc_qty += ws_alc_qty
                  stk_alocs.append({stk_mchb_id: stock.uuid, stk_mrp_id: req.uuid, alc_qty: ws_alc_qty})
                end #if stock.bal_qty > 0
                break if req.bal_qty == 0
              end #stocks.each do |stock|
            end #if stocks.present?
            break if req.bal_qty == 0
          end # sto["#{werks}"].each
        end
      end
    end
    #matnr,werks,charg,clabs,cumlm,cinsm,ceinm,cspem,cretm,budat,bal_qty,alc_qty,lgort,uuid

    stk_mchbs = stock_hash.values.flatten
    while stk_mchbs.present?
      values = []
      stk_mchbs.pop(500).each do |row|
        puts "#{row.matnr}"
        values.append("select '#{row.matnr}','#{row.werks}','#{row.charg}','#{row.clabs}','#{row.cumlm}','#{row.cinsm}','#{row.ceinm}','#{row.cspem}','#{row.cretm}','#{row.budat}','#{row.bal_qty}','#{row.alc_qty}','#{row.lgort}','#{row.uuid}',#{row.lbkum},#{row.salk3} from dual")
      end
      sql = "insert into tmplum.stk_mchb(matnr,werks,charg,clabs,cumlm,cinsm,ceinm,cspem,cretm,budat,bal_qty,alc_qty,lgort,uuid,lbkum,salk3) #{values.join(' union all ')}"
      Db.connection.execute(sql)
    end

    # puts "update tmplum.mchbx"
    # while stk_mchbs.present?
    #   values = []
    #   stk_mchbs.pop(2).each do |row|
    #     values.append ("select '#{row.uuid}' uuid, #{row.alc_qty} alc_qty, #{row.bal_qty} bal_qty from dual")
    #   end
    #   sql = "merge into tmplum.mchbx a using (( #{values.join(' union all ')})) b on (b.uuid = a.uuid) when matched then update set a.alc_qty = b.alc_qty, a.bal_qty = b.bal_qty"
    #   Db.connection.execute(sql)
    # end


    #werks,matnr,delkz,delnr,del12,delps,delet,dat00,mng01,bal_qty,alc_qty,uuid
    puts "insert stk_mrp"
    while reqs.present?
      values = []
      reqs.pop(500).each do |row|
        values.append("select '#{row.werks}','#{row.matnr}','#{row.delkz}','#{row.delnr}','#{row.del12}','#{row.delps}','#{row.delet}','#{row.dat00}','#{row.mng01}','#{row.bal_qty}','#{row.alc_qty}','#{row.uuid}','#{row.kunnr}' from dual")
      end
      sql = "insert into tmplum.stk_mrp(werks,matnr,delkz,delnr,del12,delps,delet,dat00,mng01,bal_qty,alc_qty,uuid,kunnr) #{values.join(' union all ')}"
      Db.connection.execute(sql)
    end

    #stk_mchb_id,stk_mrp_id,alc_qty
    puts "insert stk_aloc"
    while stk_alocs.present?
      values = []
      stk_alocs.pop(500).each do |row|
        values.append("select '#{row[:stk_mchb_id]}','#{row[:stk_mrp_id]}','#{row[:alc_qty]}' from dual")
      end
      sql = "insert into tmplum.stk_aloc(stk_mchb_id,stk_mrp_id,alc_qty) #{values.join(' union all ')}"
      Db.connection.execute(sql)
    end

  end

end