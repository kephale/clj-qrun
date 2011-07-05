(flatten
 ;; Lawnmower run
 (for [[lawn-rows limit] {12 150}
       ins [:tag]]
   (list
    (repeat 100   {:name (str "lawnmower-dimensions_8x" lawn-rows "-instructions_" ins)
		   :command (list "java" "-jar" "/home/ci/kyle/Clojush/clojush-20110118-kephale.18-standalone.jar" "examples.lawnmower" (str "{:lawn-dimensions [8 " lawn-rows "] :limit " limit " :instruct\
ion-set " ins " :node-selection-method :leaf-probability :mutation-probability 0.45 :crossover-probability 0.45 :simplification-probability 0 :report-simplifications 0}"))
		   :lawn-dimensions [8 lawn-rows]
		   :limit limit
		   :instruction-set ins}))))
