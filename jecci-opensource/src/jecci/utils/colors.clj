(ns jecci.utils.colors)

(def cid 0)

(def colors
  "colors that can be used to plot, remember to remove the color in the set
  when it is being used. should be enough, jepsen can only render 12 nemeses at most"
  [
   "#FFE4C4"
   "#FFF8DC"
   "#FFFACD"
   "#F0FFF0"
   "#E6E6FA"
   "#FFF0F5"
   "#FFE4E1"
   "#000080"
   "#00CED1"
   "#00FFFF"
   "#7FFFD4"
   "#20B2AA"
   "#00FF7F"
   "#228B22"
   "#CD5C5C"
   "#F4A460"
   "#FF00FF"
   "#9932CC"
   "#00F5FF"
   ])

(defn ret-rm-color
  "return a color and remove it from color collections"
  []
  (if (>= cid (count colors))
    (throw (Exception. "not enough colors")))
  (let [_cid cid]
    (def cid (inc cid))
    (nth colors cid)))