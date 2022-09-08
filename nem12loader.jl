using DataFrames

_100row=["RecordIndicator","VersionHeader","DateTime","FromParticipant","ToParticipant"]
_200row=["RecordIndicator","NMI","NMIConfiguration","RegisterID","NMISuffix","MDMDataStreamIdentifier","MeterSerialNumber","UOM","IntervalLength","NextScheduledReadDate"]
_300row=["RecordIndicator","IntervalDate","QualityMethod","ReasonCode","ReasonDescription","UpdateDateTime","MSATSLoadDateTime"]
_400row=["RecordIndicator","StartInterval","EndInterval","QualityMethod","ReasonCode","ReasonDescription"]
df = DataFrame()
dfs = []
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
            #Creating core info columns, icrement new rows if there is new NMI or new IntervalDate
            if ("NMI" in names(df))
                if ((cmp(last(df[!,"NMI"]),NMI)!=0)||last(df[!,"IntervalDate"])!=IntervalDate ) # a df only store the data of one NMI one day
                    push!(dfs,df)
                    global df= DataFrame()
                    df[!,"NMI"]=fill(NMI,Numbers_of_Intervals)
                    df[!,"IntervalDate"]=fill(IntervalDate,Numbers_of_Intervals)
                    df[!,"Interval"]=collect(1:Numbers_of_Intervals)
                end
            else
            #initialize df if it is empty
                df[!,"NMI"]=fill(NMI,Numbers_of_Intervals)
                df[!,"IntervalDate"]=fill(IntervalDate,Numbers_of_Intervals)
                df[!,"Interval"]=collect(1:Numbers_of_Intervals)
            end
            #append to the current df 
            df[!,"$NMISuffix"]=line[3:3+Numbers_of_Intervals-1]
            df[!,"Quality_$NMISuffix"]=fill(QualityMethod,Numbers_of_Intervals)
        elseif (cmp(line[1],"400")==0)            
            #for 400 row, update the QualityMethod 
            NMISuffix=_200Record["NMISuffix"]
            _400Record=Dict(_400row.=>line)
            StartInterval=parse(Int64,_400Record["StartInterval"])
            EndInterval=parse(Int64,_400Record["EndInterval"])
            QualityMethod=_400Record["QualityMethod"]
            println("StartInterval",StartInterval,"EndInterval",EndInterval,"QualityMethod",QualityMethod)
            # df[(df[!,"Interval"].>=StartInterval)&&(df[!,"Interval"].<=EndInterval),"Quality_$NMISuffix"].=QualityMethod
        elseif (cmp(line[1],"500")==0)            
            println("500 row")
        elseif (cmp(line[1],"900")==0)            
            println("900 row")
        else
            println("unknown row")
        end
    end
    push!(dfs,df)
    println(dfs)
end