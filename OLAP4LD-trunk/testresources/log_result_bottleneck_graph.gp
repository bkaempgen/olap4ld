# gnuplot script file for plotting bandwidth over time
#!/usr/bin/gnuplot
reset
set terminal png size 1024,680
set size 1,0.5
set origin 0,0
set output "log_result_bottleneck_graph.png"
#set terminal postscript portrait enhanced color solid lw 2.0 "Helvetica" 14 
#set output "force.ps"

set style data histograms
set style histogram rowstacked

#set key outside

#set yrange [0:8000]

set boxwidth 0.5 relative
set style fill solid 1.0 border -1
set datafile separator "|"

set xtics rotate
set xlabel "Queries"
set ylabel "Elapsed query time in ms"
#set title  "Elapsed query time per component" 

plot "< sqlite3 /home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/testresources/bottleneck.db 'select querytime,queryname,triples,avg(loadingvalidatingdataset),avg(generatinglogicalqueryplan),avg(executinglogicalqueryplan) from bottleneck where queryname like \" test%\" group by queryname,triples order by triples, queryname desc'" using 4 t "Loading and validating dataset", '' using 5:xticlabels(2) t "Generating query plan", '' using 6:xticlabels(2) t "Executing query plan"
