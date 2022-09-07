using DataFrames

_100row=["RecordIndicator","VersionHeader","DateTime","FromParticipant","ToParticipant"]
_200row=["RecordIndicator","NMI","NMIConfiguration","RegisterID","NMISuffix","MDMDataStreamIdentifier","MeterSerialNumber","UOM","IntervalLength","NextScheduledReadDate"]
_300row=["RecordIndicator","IntervalDate","QualityMethod","ReasonCode","ReasonDescription","UpdateDateTime","MSATSLoadDateTime"]
        df = DataFrame()
open("NEM12.csv") do file
    for ln in eachline(file)
        line=split(ln,[','])
        if (cmp(line[1],"100")==0)
            #remove the empty appendix if there is a tailing comma
            if (length(line)>=6)
                pop!(line)
            end
            try
                _100Record=Dict(_100row.=>line)
            catch
                println("too many items in the 100 row, check if it is a valid NEM12 file")
            end
        elseif (cmp(line[1],"200")==0)
            if (length(line)>=11)
                pop!(line)
            end
            # println(_200Record)
            try
                global _200Record=Dict(_200row.=>line)
            catch
                println("too many items in the 200 row, check if it is a valid NEM12 file")
            end
        elseif (cmp(line[1],"300")==0)
            Numbers_of_Intervals=convert(Int64,24*60/parse(Int64,_200Record["IntervalLength"]))
            NMISuffix=_200Record["NMISuffix"]
            NMI=_200Record["NMI"]
            QualityMethod=line[3+Numbers_of_Intervals]
            IntervalDate=parse(Int64,line[2])
            #Creating core info columns
            df[!,"NMI"]=fill(NMI,Numbers_of_Intervals)
            df[!,"IntervalDate"]=fill(IntervalDate,Numbers_of_Intervals)
            df[!,"Interval"]=collect(1:Numbers_of_Intervals)
            #append to the 
            # df[!,"$NMISuffix"]=parse(Float64,line[3:3+Numbers_of_Intervals-1])
            df[!,"$NMISuffix"]=line[3:3+Numbers_of_Intervals-1]
            df[!,"Quality_$NMISuffix"]=fill(QualityMethod,Numbers_of_Intervals)
        elseif (cmp(line[1],"400")==0)            
            println("400 row")
        elseif (cmp(line[1],"500")==0)            
            println("500 row")
        elseif (cmp(line[1],"900")==0)            
            println("900 row")
        else
            println("unknown row")
        end
        println(df)
    end
end