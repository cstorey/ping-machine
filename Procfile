node-a: unbuffer stack exec -- stuff-server +RTS -t -T -s -S -RTS 9990 9991 9993 9995 | (mkdir -vp log/a/ && svlogd -ttt -v log/a/)
node-b: unbuffer stack exec -- stuff-server +RTS -t -T -s -S -RTS 9992 9993 9991 9995 | (mkdir -vp log/b/ && svlogd -ttt -v log/b/)
node-c: unbuffer stack exec -- stuff-server +RTS -t -T -s -S -RTS 9994 9995 9991 9993 | (mkdir -vp log/c/ && svlogd -ttt -v log/c/)
