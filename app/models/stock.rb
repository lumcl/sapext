class Stock

  def self.main
    allocation
    stk_report
  end

  def self.allocation
    #Db.connection.execute("truncate table stk_mchb_log")
    Db.connection.execute("insert into stk_mchb_log select * from stk_mchb where to_char(created_at,'YYYYMMDD') < to_char(sysdate,'YYYYMMDD')")
    Db.connection.execute('truncate table stk_mchb')
    Db.connection.execute("insert into stk_mrp_log select * from stk_mrp where to_char(created_at,'YYYYMMDD') < to_char(sysdate,'YYYYMMDD')")
    Db.connection.execute('truncate table stk_mrp')
    Db.connection.execute("insert into stk_aloc_log select * from stk_aloc where to_char(created_at,'YYYYMMDD') < to_char(sysdate,'YYYYMMDD')")
    Db.connection.execute('truncate table stk_aloc')

    sto = {}
    # sto['101A'] = %w[101A 701A 381A 481A 111A 921A 482A]
    # sto['111A'] = %w[111A 282A 101A.L106 101A.L128]
    # sto['112A'] = %w[112A]
    # sto['281A'] = %w[281A]
    # sto['282A'] = %w[282A 111A]
    # sto['381A'] = %w[381A 701A 101A.RM03 101A.RM04 101A.RM05 101A.RM06 101A.FGR2 101A.L107 101A.L108 101A.L110 101A.L112 101A.L117 101A.L118 101A.L119 101A.L123 101A.L124 101A.L125 101A.L128 101A.L129 101A.L130 101A.L131 101A.L132 101A.L133 101A.L136 101A.L137 101A.L138 101A.PHTX 921A]
    # sto['382A'] = %w[382A]
    # sto['481A'] = %w[481A 101A.RM01 101A.FGR1 101A.L101 101A.L109 101A.L111 101A.L134 101A.L135 101A.PHDT]
    # sto['482A'] = %w[482A 101A.RM02]
    # sto['921A'] = %w[921A 101A 481A]
    # sto['701A'] = %w[701A 381A]

    sto['101A'] = %w[101A 701A 381A 481A 111A 921A 482A]
    sto['111A'] = %w[111A 282A]
    sto['112A'] = %w[112A]
    sto['281A'] = %w[281A]
    sto['282A'] = %w[282A 111A]
    sto['381A'] = %w[381A 701A 101A.RM03A 101A.RM04 101A.RM05 101A.RM06 101A.PHTX]
    sto['382A'] = %w[382A]
    sto['481A'] = %w[481A 101A.RM01 101A.PHDT]
    sto['482A'] = %w[482A 101A.RM02]
    sto['921A'] = %w[921A 101A 481A 701A 381A]
    sto['701A'] = %w[701A 381A]

    sto_lgorts = sto.values.flatten.select{|a| a.size > 4}

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
      select a.matnr,a.werks,a.charg,a.lgort,a.budat,a.bal_qty,a.alc_qty,a.uuid,
             a.clabs,a.cumlm,a.cinsm,a.ceinm,a.cspem,a.cretm,a.lbkum,a.salk3,
             case
               when werks = '101A' and c.werks in ('111A','112A') then 'PH'
               when werks = '101A' and c.werks in ('481A','482A') then 'DT'
               when werks = '101A' and lgort in ('RM01','RM02','FGR1','L101','L109','L111','L134','L135','PHDT','L127') then 'DT'
               when werks = '101A' and lgort in ('L106') then 'PH'
               when werks in ('481A','482A') then 'DT'
               when werks in ('111A','112A','282A') then 'PH'
               else 'TX'
             end vtweg
        from tmplum.mchbx a
          left join tmplum.mch1x b on b.matnr=a.matnr and b.charg=a.charg
          left join sapsr3.mseg  c on c.mandt='168' and c.mjahr=b.mjahr and c.mblnr=b.mblnr and c.zeile=b.zeile
    "
    Db.find_by_sql(sql).each do |row|
      sto_lgort = sto_lgorts.include?("#{row.werks}.#{row.lgort}") ? row.lgort : '****'
      array = stock_hash.key?("#{row.matnr}.#{row.werks}.#{sto_lgort}") ? stock_hash["#{row.matnr}.#{row.werks}.#{sto_lgort}"] : []
      array.append(row)
      stock_hash["#{row.matnr}.#{row.werks}.#{sto_lgort}"] = array
    end

    stk_alocs = []
    sql = "
      select werks,matnr,delkz,delnr,del12,delps,delet,dat00,lgort,
             mng01,mng01 bal_qty,0 alc_qty,rawtohex(sys_guid()) uuid, kunnr
        from sapsr3.zsd0012
      where mandt='168' and mng01 > 0 and plumi='-' and delkz not in ('U1','U2','U3')
            and kunnr not in ('L100','L111','L210','L300','L400','L700','L920')
    "
    reqs = Db.find_by_sql(sql)
    reqs.group_by(&:matnr).each do |matnr, werks|
      werks.group_by(&:werks).each do |werks, rows|
        rows.each do |req|
          sto["#{werks}"].each do |supply_werks|
            if sto_lgorts.include?("#{supply_werks}.#{req.lgort}")
              stocks = stock_hash["#{matnr}.#{supply_werks}.#{req.lgort}"]
            else
              stocks = supply_werks.size == 4 ? stock_hash["#{matnr}.#{supply_werks}.****"] : stock_hash["#{matnr}.#{supply_werks[0..3]}.#{supply_werks[5..8]}"]
            end
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
        values.append("select '#{row.matnr}','#{row.werks}','#{row.charg}','#{row.clabs}','#{row.cumlm}','#{row.cinsm}','#{row.ceinm}','#{row.cspem}','#{row.cretm}','#{row.budat}','#{row.bal_qty}','#{row.alc_qty}','#{row.lgort}','#{row.uuid}',#{row.lbkum},#{row.salk3},'#{row.vtweg}' from dual")
      end
      sql = "insert into tmplum.stk_mchb(matnr,werks,charg,clabs,cumlm,cinsm,ceinm,cspem,cretm,budat,bal_qty,alc_qty,lgort,uuid,lbkum,salk3,vtweg) #{values.join(' union all ')}"
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

  def self.stk_report
    Db.connection.execute('truncate table stk_report')
    sql = "
      insert into stk_report
      with
        z_knvp as
          (  select kunnr, min (decode (parvw, 'YA', concat ( nchmc, vnamc))) armgr,
                    min (decode (parvw, 'Y2', concat ( nchmc, vnamc))) srep2,
                    min (decode (parvw, 'YB', concat ( nchmc, vnamc))) acc_sup_1,
                    min (decode (parvw, 'YC', concat ( nchmc, vnamc))) acc_sup_2
               from sapsr3.knvp, sapsr3.pa0002
              where     knvp.mandt = '168'
                    and pa0002.mandt = knvp.mandt
                    and pa0002.pernr = knvp.pernr
             group by kunnr),
        z_t001w as
          ( select a.werks,b.bukrs,c.waers,nvl(d.tcurr,'RMB') tcurr,
                   case when c.waers in ('TWD','JPY') then nvl(d.ukurs,1) * 100 else nvl(d.ukurs,1) end ukurs
              from sapsr3.t001w a
                join sapsr3.t001k  b on b.mandt='168' and b.bwkey=a.bwkey
                join sapsr3.t001   c on c.mandt='168' and c.bukrs=b.bukrs
                left join tmplum.tcurrx d on d.kurst='C' and d.fcurr=c.waers and d.tcurr='RMB'
              where a.mandt='168'),
        z_open_po as
          ( select a.matnr,a.werks,sum(mng01) open_po
              from sapsr3.zsd0012 a
              where a.mandt='168' and a.delkz in ('BE','LA')
                and (a.matnr,a.werks) in (select distinct matnr,werks from tmplum.stk_mchb)
              group by a.matnr,a.werks),
        z_knmt as
          ( select matnr,vtweg,kunnr
              from (select a.matnr,a.vtweg,a.kunnr,a.erdat,
                       rank() over (partition by a.matnr,a.vtweg order by a.erdat desc, a.rowid desc) rank1
                      from sapsr3.knmt a
                      where a.mandt='168' and a.matnr in (select distinct matnr from stk_mchb) and a.vtweg <> '00')
              where rank1=1)

      select /*+ index(d \"MAKT~0\") index(f \"LFA1~0\") index(i \"MARAX_PK\") */
             a.matnr,
             a.werks,
             a.vtweg FL,
             g.ktext PL,
             nvl(q.sr,'OT') SR,
             a.charg,
             a.lgort,
             a.budat,
             b.meins,
             (a.bal_qty + a.alc_qty) stk_qty,
             a.alc_qty,
             a.bal_qty idle_qty,
             h.tcurr curr,
             case when a.lbkum = 0 then 0 else round((((a.bal_qty + a.alc_qty) * a.salk3) / a.lbkum) * h.ukurs, 2) end stk_amt,
             case when a.lbkum = 0 then 0 else round(((a.bal_qty * a.salk3) / a.lbkum) * h.ukurs, 2) end idle_amt,
             round((sysdate - to_date(a.budat,'YYYYMMDD')),0) stk_day,
             case
               when round((sysdate - to_date(a.budat,'YYYYMMDD')),0) <= 7 then '007'
               when round((sysdate - to_date(a.budat,'YYYYMMDD')),0) <= 15 then '015'
               when round((sysdate - to_date(a.budat,'YYYYMMDD')),0) <= 30 then '030'
               when round((sysdate - to_date(a.budat,'YYYYMMDD')),0) <= 60 then '060'
               when round((sysdate - to_date(a.budat,'YYYYMMDD')),0) <= 90 then '090'
               when round((sysdate - to_date(a.budat,'YYYYMMDD')),0) <= 180 then '180'
               when round((sysdate - to_date(a.budat,'YYYYMMDD')),0) <= 360 then '360'
               else '360+'
             end stk_aged,
             case
               when a.lgort like 'LD%' then '1. LED'
               when a.werks in ('481A','482A') and b.matkl like 'OL%' then '1. LED'
               when a.werks = '101A' and a.lgort in ('RM01','RM02','FGR1','L101','L109','L111','L134','L135','PHDT') and b.matkl like 'OL%' then '1. LED'
               when c.ebeln like '88%' then '2. 策采政備'
               when c.ebeln like '87%' then '3. 業務政備'
               else '4. MRP'
             end stk_cat,
             b.matkl,
             case when b.matkl in ('STH','ST') then 'ZROH' else b.mtart end mtart,
             d.maktx,
             c.bwart,
             c.mjahr||'.'||c.mblnr||'.'|| c.zeile mb51,
             case when c.ebeln <> ' ' then c.ebeln ||'.'|| c.ebelp else ' 'end po,
             c.lifnr supplier,
             f.sortl sup_name,
             e.ekgrp,
             e.plifz leadtime,
             e.bstrf moq,
             e.eisbe safety,
             e.disgr mrp_grp,
             c.aufnr mo,
             nvl(p.kunnr,i.kunnr) customer,
             j.sortl cust_name,
             j.name1 cust_comp,
             m.armgr acc_mgr,
             m.srep2 acc_rep,
             m.acc_sup_1,
             m.acc_sup_2,
             q.vkgrp,
             k.created_at z_datecode,
             k.kunnr z_customer,
             nvl(k.acc_rep,nvl(k.ecn_resp,moq_resp)) z_owner,
             k.moq_lifnr z_oldlotno,
             k.reason z_respcode,
             k.vbeln z_resptxt,
             k.review_date z_respdat,
             l.open_po
        from tmplum.stk_mchb a
          join sapsr3.mara b on b.mandt='168' and b.matnr=a.matnr
          join sapsr3.makt d on d.mandt='168' and d.matnr=a.matnr and d.spras='M'
          join sapsr3.marc e on e.mandt='168' and e.matnr=a.matnr and e.werks=a.werks
          join z_t001w h on h.werks=a.werks
          left join tmplum.mch1x c on c.matnr=a.matnr and c.charg=a.charg
          left join sapsr3.lfa1 f on f.mandt='168' and f.lifnr=c.lifnr
          left join sapsr3.cepct g on g.mandt='168' and g.spras='M' and g.prctr=e.prctr and g.datbi='99991231' and g.kokrs='3058'
          left join tmplum.marax i on i.matnr=a.matnr
          left join tmplum.stk_idle k on k.matnr=a.matnr and k.charg=a.charg
          left join z_open_po l on l.matnr=a.matnr and l.werks=a.werks
          left join z_knmt p on p.matnr=a.matnr and p.vtweg=a.vtweg
          left join z_knvp m on m.kunnr=nvl(p.kunnr,i.kunnr)
          left join sapsr3.kna1 j on j.mandt='168' and j.kunnr=nvl(p.kunnr,i.kunnr)
          left join tmplum.knvvx q on q.kunnr=nvl(p.kunnr,i.kunnr)
    "
    Db.connection.execute(sql)

    sql = "
      insert into stk_idle(matnr,charg,fl,pl,sr,werks,kunnr,acc_rep,acc_sup_1,acc_sup_2,moq_lifnr,moq,reason,remark,review_date,vbeln,posnr)
      select distinct a.matnr,
             a.charg,
             a.fl,
             a.pl,
             a.sr,
             a.werks,
             a.customer kunnr,
             a.acc_rep,
             a.acc_sup_1,
             a.acc_sup_2,
             a.supplier moq_lifnr,
             a.moq moq,
             b.reason,
             to_char(b.remark) remark,
             b.review_date,
             b.vbeln,
             b.posnr
        from tmplum.stk_report a
          left join tmplum.stk_idle b on b.matnr=a.matnr and b.charg=a.charg
        where a.idle_qty > 0 and (a.matnr,a.charg) not in (select matnr,charg from stk_idle)
    "
    Db.connection.execute(sql)
  end

end