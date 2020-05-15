package anonymizer;

import org.deidentifier.arx.ARXAnonymizer;
import org.deidentifier.arx.ARXConfiguration;
import org.deidentifier.arx.ARXResult;
import org.deidentifier.arx.Data;
import org.deidentifier.arx.aggregates.StatisticsQuality;
import org.deidentifier.arx.aggregates.quality.QualityMeasureColumnOriented;
import org.deidentifier.arx.criteria.KAnonymity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Optimizer {

    private Data data;
    private String[] quasiIdentifier;
    private HashMap<String, HashMap<String, ArrayList<String>>> workloadUtility;
    private ArrayList<String> qidToOptimize;
    private ArrayList<String> qidNotUsed;
    private HashMap<String,Integer> mapQIDlevel;
    private ARXConfiguration config;

    public Optimizer(ARXConfiguration config, Data data, String[] quasiIdentifier, HashMap<String, HashMap<String, ArrayList<String>>> workloadUtility) {
        this.config = config;
        this.data = data;
        this.quasiIdentifier = quasiIdentifier;
        this.workloadUtility = workloadUtility;
        this.qidToOptimize = new ArrayList<>();
        this.qidNotUsed = new ArrayList<>();
        this.mapQIDlevel = new HashMap<>();
    }

    public void execute() {
        boolean[] isInWorkload = new boolean[quasiIdentifier.length];
        qidToOptimize.clear();
        qidNotUsed.clear();
        for (int i = 0; i < quasiIdentifier.length; i++) {
            isInWorkload[i] = workloadUtility.containsKey(quasiIdentifier[i].replace("-", "_").toUpperCase());
        }
        for (int i = 0; i < isInWorkload.length; i++) {
            if (isInWorkload[i]) {
                qidToOptimize.add(quasiIdentifier[i]);
            } else {
                qidNotUsed.add(quasiIdentifier[i]);
            }
        }
        for (String qid : qidToOptimize) {
            HashMap<String, ArrayList<String>> conditionMap = workloadUtility.get(qid.replace("-","_").toUpperCase());
            ArrayList<String> values = new ArrayList<>();
            for (ArrayList<String> val : conditionMap.values()) {
                values.addAll(val);
            }
            int level = 999;
            for (String target : values) {
                int temp = getValueLevelInHierarchy(data.getDefinition().getHierarchy(qid),target);
                if (level>temp){
                    level = temp;
                }
            }
            if (level==999){
                level=0;
            }
            data.getDefinition().setMaximumGeneralization(qid, level);
            data.getDefinition().setMinimumGeneralization(qid, level);
        }
        if (qidToOptimize.isEmpty()){
            for (String qid: qidNotUsed){
                data.getDefinition().setMaximumGeneralization(qid,data.getDefinition().getHierarchy(qid)[0].length - 1);
                data.getDefinition().setMinimumGeneralization(qid,data.getDefinition().getHierarchy(qid)[0].length - 1);
            }
        } else if (!findBestCombinationForQIDnotUsed()){
            System.out.println("No Optimization found for not loosing useful QID information.");
            System.out.println("Searching for higher level hierarchy...");
            findBestCombinationForUsefulQI();
        }
    }

    private void findBestCombinationForUsefulQI() {
        mapQIDlevel.clear();
        ArrayList<ArrayList<String>> suppList = new ArrayList<>();
        for (String qid:qidToOptimize) {
            int maxHierLevel = data.getDefinition().getHierarchy(qid)[0].length - 1;
            ArrayList<String> values = new ArrayList<>();
            for(int i = 0; i<=maxHierLevel;i++){
                values.add(String.valueOf(i));
            }
            suppList.add(values);
        }
        ArrayList<String> permutations = new ArrayList<>();
        generatePermutations(suppList,permutations,0,"");
        double best = 0.0;
        String[] bestHier = new String[qidToOptimize.size()];
        for(String s: permutations){
            String[] hierValues = s.split(" ");
            for(int i=0; i<hierValues.length;i++){
                data.getDefinition().setMaximumGeneralization(qidToOptimize.get(i), Integer.parseInt(hierValues[i]));
                data.getDefinition().setMinimumGeneralization(qidToOptimize.get(i), Integer.parseInt(hierValues[i]));
            }
            double temp = anonymize();
            if (temp>best) {
                best = temp;
                bestHier = hierValues;
            }
        }
        for(int i=0; i<bestHier.length;i++){
            data.getDefinition().setMaximumGeneralization(qidToOptimize.get(i), Integer.parseInt(bestHier[i]));
            data.getDefinition().setMinimumGeneralization(qidToOptimize.get(i), Integer.parseInt(bestHier[i]));
        }
        for (String qid: qidNotUsed){
            data.getDefinition().setMaximumGeneralization(qid,data.getDefinition().getHierarchy(qid)[0].length - 1);
            data.getDefinition().setMinimumGeneralization(qid,data.getDefinition().getHierarchy(qid)[0].length - 1);
        }
    }

    private boolean findBestCombinationForQIDnotUsed() {
        mapQIDlevel.clear();
        ArrayList<ArrayList<String>> suppList = new ArrayList<>();
        for (String qid:qidNotUsed) {
            int maxHierLevel = data.getDefinition().getHierarchy(qid)[0].length - 1;
            ArrayList<String> values = new ArrayList<>();
            for(int i = 0; i<=maxHierLevel;i++){
                values.add(String.valueOf(i));
            }
            suppList.add(values);
        }
        ArrayList<String> permutations = new ArrayList<>();
        generatePermutations(suppList,permutations,0,"");
        double best = 0.0;
        String[] bestHier = new String[qidNotUsed.size()];
        for(String s: permutations){
            String[] hierValues = s.split(" ");
            for(int i=0; i<hierValues.length;i++){
                data.getDefinition().setMaximumGeneralization(qidNotUsed.get(i), Integer.parseInt(hierValues[i]));
                data.getDefinition().setMinimumGeneralization(qidNotUsed.get(i), Integer.parseInt(hierValues[i]));
            }
            double temp = anonymize();
            if (temp>best) {
                best = temp;
                bestHier = hierValues;
            }
        }
        if(best==0.0){
            return false;
        }
        for(int i=0; i<bestHier.length;i++){
            data.getDefinition().setMaximumGeneralization(qidNotUsed.get(i), Integer.parseInt(bestHier[i]));
            data.getDefinition().setMinimumGeneralization(qidNotUsed.get(i), Integer.parseInt(bestHier[i]));
        }
        return true;
    }

    private void generatePermutations(ArrayList<ArrayList<String>> lists, ArrayList<String> result, int depth, String current) {
        if (depth == lists.size()) {
            result.add(current.trim());
            return;
        }

        for (int i = 0; i < lists.get(depth).size(); i++) {
            generatePermutations(lists, result, depth + 1, current + lists.get(depth).get(i) + " ");
        }
    }


    private double anonymize(){
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        // Execute the algorithm
        try {
            data.getHandle().release();
            ARXResult result = anonymizer.anonymize(data, config);
            if(result.getGlobalOptimum()==null){
                return 0.0;
            }
            return getMetricsValue(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0.0;

    }

    private double getMetricsValue(ARXResult result) {
        StatisticsQuality utility = result.getOutput(result.getGlobalOptimum(), false).getStatistics().getQualityStatistics();
        QualityMeasureColumnOriented generalization = utility.getGeneralizationIntensity();
        double prec = 0.0;
        for(String s: qidToOptimize){
            if (generalization.isAvailable(s)){
                prec += generalization.getValue(s);
            }
        }
        prec = prec/qidToOptimize.size()*1.0;
        QualityMeasureColumnOriented nonuniformentropy = utility.getNonUniformEntropy();
        double nuentropy = 0.0;
        for(String s: qidToOptimize){
            if (nonuniformentropy.isAvailable(s)){
                nuentropy += nonuniformentropy.getValue(s);
            }
        }
        nuentropy = nuentropy/qidToOptimize.size()*1.0;
        return (prec+nuentropy)/2.0;
    }

    private int getValueLevelInHierarchy(String[][] matrix, String target) {
        if (matrix == null || matrix.length == 0 || matrix[0].length == 0) {
            return 999;
        }
        int n = matrix[0].length;
        for (String[] strings : matrix) {
            for (int j = 0; j < n; j++) {
                if (strings[j].equals(target)) {
                    return j;
                }
            }
        }
        return 999;
    }

    public Data getData() {
        return data;
    }

    public ArrayList<String> getQidOptimized() {
        return qidToOptimize;
    }
}
