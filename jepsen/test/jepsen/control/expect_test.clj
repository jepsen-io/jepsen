(ns jepsen.control.expect-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [jepsen.control.expect :as e])
  (:import (java.io PipedInputStream
                    PipedOutputStream)))

(use-fixtures :once
              (fn [run-tests]
                (with-redefs [e/decode-buffer-size 8]
                  (run-tests))))

(defn pipe
  "Returns an input and output stream pair, connected to each other."
  []
  (let [in (PipedInputStream.)
        out (PipedOutputStream.)]
    (.connect in out)
    [in out]))

(defn remote-process
  "Simulates a remote process which reads data from stdin and accumulates it in
  a buffer, as well as emitting the given stdout and stderr as strings.
  Launches a future which resolves to the stdin buffer."
  [in out err out-str err-str]
  (future
    (let [in-worker  (future (bs/to-string in))
          out-worker (future (bs/transfer out-str out)
                             (.close out))
          err-worker (future (bs/transfer err-str err)
                             (.close err))]
      @out-worker
      @err-worker
      @in-worker)))

(def basic-string-bits
  "A few simple string fragments"
  [""
   "meow"
   "woof"
   "\n"])

(def interesting-string-bits
  "A few more interesting string fragments"
  ["!@#$%^&*()`~"
   "\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f",
  "",
  "\t\u000b\f              ​    　",
  "­؀؁؂؃؄؅؜۝܏᠎​‌‍‎‏‪‫‬‭‮⁠⁡⁢⁣⁤⁦⁧⁨⁩⁪⁫⁬⁭⁮⁯﻿￹￺￻𑂽𛲠𛲡𛲢𛲣𝅳𝅴𝅵𝅶𝅷𝅸𝅹𝅺󠀁󠀠󠀡󠀢󠀣󠀤󠀥󠀦󠀧󠀨󠀩󠀪󠀫󠀬󠀭󠀮󠀯󠀰󠀱󠀲󠀳󠀴󠀵󠀶󠀷󠀸󠀹󠀺󠀻󠀼󠀽󠀾󠀿󠁀󠁁󠁂󠁃󠁄󠁅󠁆󠁇󠁈󠁉󠁊󠁋󠁌󠁍󠁎󠁏󠁐󠁑󠁒󠁓󠁔󠁕󠁖󠁗󠁘󠁙󠁚󠁛󠁜󠁝󠁞󠁟󠁠󠁡󠁢󠁣󠁤󠁥󠁦󠁧󠁨󠁩󠁪󠁫󠁬󠁭󠁮󠁯󠁰󠁱󠁲󠁳󠁴󠁵󠁶󠁷󠁸󠁹󠁺󠁻󠁼󠁽󠁾󠁿",
  "﻿",
  "￾",
  "Ω≈ç√∫˜µ≤≥÷",
  "åß∂ƒ©˙∆˚¬…æ",
  "œ∑´®†¥¨ˆøπ“‘",
  "¡™£¢∞§¶•ªº–≠",
  "¸˛Ç◊ı˜Â¯˘¿",
  "ÅÍÎÏ˝ÓÔÒÚÆ☃",
  "Œ„´‰ˇÁ¨ˆØ∏”’",
  "`⁄€‹›ﬁﬂ‡°·‚—±",
  "⅛⅜⅝⅞",
  "ЁЂЃЄЅІЇЈЉЊЋЌЍЎЏАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдежзийклмнопрстуфхцчшщъыьэюя",
  "٠١٢٣٤٥٦٧٨٩",
  "⁰⁴⁵",
  "₀₁₂",
  "⁰⁴⁵₀₁₂",
  "ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็ ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็ ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็",
    "❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙",
  "✋🏿 💪🏿 👐🏿 🙌🏿 👏🏿 🙏🏿",
  "👨‍👩‍👦 👨‍👩‍👧‍👦 👨‍👨‍👦 👩‍👩‍👧 👨‍👦 👨‍👧‍👦 👩‍👦 👩‍👧‍👦",
  "🚾 🆒 🆓 🆕 🆖 🆗 🆙 🏧",
  "0️⃣ 1️⃣ 2️⃣ 3️⃣ 4️⃣ 5️⃣ 6️⃣ 7️⃣ 8️⃣ 9️⃣ 🔟",
  "ثم نفس سقطت وبالتحديد،, جزيرتي باستخدام أن دنو. إذ هنا؟ الستار وتنصيب كان. أهّل ايطاليا، بريطانيا-فرنسا قد أخذ. سليمان، إتفاقية بين ما, يذكر الحدود أي بعد, معاملة بولندا، الإطلاق عل إيو.",
  "בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ"
  "Z̮̞̠͙͔ͅḀ̗̞͈̻̗Ḷ͙͎̯̹̞͓G̻O̭̗̮",
  "˙ɐnbᴉlɐ ɐuƃɐɯ ǝɹolop ʇǝ ǝɹoqɐl ʇn ʇunpᴉpᴉɔuᴉ ɹodɯǝʇ poɯsnᴉǝ op pǝs 'ʇᴉlǝ ƃuᴉɔsᴉdᴉpɐ ɹnʇǝʇɔǝsuoɔ 'ʇǝɯɐ ʇᴉs ɹolop ɯnsdᴉ ɯǝɹo˥",
  "00˙Ɩ$-"])

(defn strings
  "A sequence of strings of increasing difficulty."
  []
  (concat basic-string-bits
          ;interesting-string-bits
          ))

(deftest simple-test
  ; Create piped channels for the remote process
  (let [[stdin-in  stdin-out]  (pipe)
        [stderr-in stderr-out] (pipe)
        [stdout-in stdout-out] (pipe)
        ; And a standard chain of Expects
        chain (-> [; We start with the remote, which we'll connect to our
                   ; process.
                   (e/remote-streams stdin-out stdout-in stderr-in)
                   ; Followed by the UTF-8 converter
                   (e/utf8)
                   ; And the accumulator
                   (e/local-accumulator)]
                  e/link!
                  e/start!)
        local (e/local-end chain)
        in-str "I'm input!"
        out-str "I'm output!"
        err-str "I'm error!"
        ; Now we spawn our remote process on the other end
        remote (remote-process stdin-in
                               stdout-out
                               stderr-out
                               out-str
                               err-str)
        ; And feed our stdin to the local expect
        _ (e/in! local in-str)
        _ (e/close-in! local)
        ; Wait for the remote process to exit
        remote-in @remote]
    ; Clean up the chain
    (e/close! chain)

    ; Great, now let's make sure the stdin got through to the remote process
    (is (= in-str remote-in))
    ; And that we fully read the stdout and stderr
    (is (= out-str (e/out-str local)))
    (is (= err-str (e/err-str local)))))
