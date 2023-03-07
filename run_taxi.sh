rm -rf storage*
mv src/freepastry.params ./
ant clean
ant init
ant
mv ./freepastry.params ./src/
sleep 1
wait
java -Dfile.encoding=UTF-8 -classpath /home/parallels/Desktop/StreamingFreePastry/classes:/home/parallels/Desktop/StreamingFreePastry/lib/bouncycastle.jar:/home/parallels/Desktop/StreamingFreePastry/lib/commons-jxpath-1.1.jar:/home/parallels/Desktop/StreamingFreePastry/lib/commons-logging.jar:/home/parallels/Desktop/StreamingFreePastry/lib/sbbi-upnplib-1.0.4.jar:/home/parallels/Desktop/StreamingFreePastry/lib/xmlpull_1_1_3_4a.jar:/home/parallels/Desktop/StreamingFreePastry/lib/xpp3-1.1.3.4d_b2.jar:/home/parallels/Desktop/StreamingFreePastry/lib/junit-4.11.jar stream.nyctaxi.TestNYC