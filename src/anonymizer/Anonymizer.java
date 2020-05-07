package anonymizer;

import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.StatisticsQuality;
import org.deidentifier.arx.aggregates.quality.QualityMeasureColumnOriented;
import org.deidentifier.arx.criteria.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Anonymizer {
    private String datasetFullPath;
    private String[] sensitiveAttributes, quasiIdentifier, quasiIdentifierHierarchyFullPath, identifyingAttributes, insensitiveAttributes;
    private char datasetDelimiter, hierarchyDelimiter;
    private Data data;
    private ARXResult result;
    private ARXConfiguration config;

    public Anonymizer(){
    }

    public Anonymizer(String datasetFullPath, String[] identifyingAttributes, String[] sensitiveAttributes, String[] insensitiveAttributes, String[] quasiIdentifier, String[] quasiIdentifierHierarchyFullPath, char datasetDelimiter, char hierarchyDelimiter){
        this.datasetFullPath = datasetFullPath;
        this.identifyingAttributes = identifyingAttributes;
        this.sensitiveAttributes = sensitiveAttributes;
        this.insensitiveAttributes = insensitiveAttributes;
        this.quasiIdentifier = quasiIdentifier;
        this.quasiIdentifierHierarchyFullPath = quasiIdentifierHierarchyFullPath;
        this.datasetDelimiter = datasetDelimiter;
        this.hierarchyDelimiter = hierarchyDelimiter;
        this.data = null;
        this.result = null;
        this.config = ARXConfiguration.create();
    }

    public boolean setKAnonymity(int k){
        PrivacyCriterion privacyCriterion = this.config.getPrivacyModel(KAnonymity.class);
        if (privacyCriterion!=null) {
            this.config.removeCriterion(privacyCriterion);
        }
        try {
            this.config.addPrivacyModel(new KAnonymity(k));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean setSuppressionLimit(double s){
        try{
            this.config.setSuppressionLimit(s);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean setOrderedDistanceTCloseness(String attribute, double t){
        boolean result = Arrays.stream(sensitiveAttributes).anyMatch(attribute::equalsIgnoreCase);
        if(result) {
            PrivacyCriterion privacyCriterion = this.config.getPrivacyModel(OrderedDistanceTCloseness.class);
            if (privacyCriterion != null) {
                this.config.removeCriterion(privacyCriterion);
            }
            try {
                this.config.addPrivacyModel(new OrderedDistanceTCloseness(attribute, t));
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return result;
    }

    public boolean setEqualDistanceTCloseness(String attribute, double t){
        boolean result = Arrays.stream(sensitiveAttributes).anyMatch(attribute::equalsIgnoreCase);
        if(result) {
            PrivacyCriterion privacyCriterion = this.config.getPrivacyModel(EqualDistanceTCloseness.class);
            if (privacyCriterion != null) {
                this.config.removeCriterion(privacyCriterion);
            }
            try {
                this.config.addPrivacyModel(new EqualDistanceTCloseness(attribute, t));
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return result;
    }

    public boolean setEntropyLDiversity(String attribute, double l){
        boolean result = Arrays.stream(sensitiveAttributes).anyMatch(attribute::equalsIgnoreCase);
        if(result) {
            PrivacyCriterion privacyCriterion = this.config.getPrivacyModel(EntropyLDiversity.class);
            if (privacyCriterion != null) {
                this.config.removeCriterion(privacyCriterion);
            }
            try {
                this.config.addPrivacyModel(new EntropyLDiversity(attribute, l));
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return result;
    }

    public boolean setDistinctLDiversity(String attribute, int l){
        boolean result = Arrays.stream(sensitiveAttributes).anyMatch(attribute::equalsIgnoreCase);
        if(result) {
            PrivacyCriterion privacyCriterion = this.config.getPrivacyModel(DistinctLDiversity.class);
            if (privacyCriterion != null) {
                this.config.removeCriterion(privacyCriterion);
            }
            try {
                this.config.addPrivacyModel(new DistinctLDiversity(attribute, l));
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return result;
    }

    public boolean setRecursiveCLDiversity(String attribute, double c, int l){
        boolean result = Arrays.stream(sensitiveAttributes).anyMatch(attribute::equalsIgnoreCase);
        if(result) {
            PrivacyCriterion privacyCriterion = this.config.getPrivacyModel(RecursiveCLDiversity.class);
            if (privacyCriterion != null) {
                this.config.removeCriterion(privacyCriterion);
            }
            try {
                this.config.addPrivacyModel(new RecursiveCLDiversity(attribute, c, l));
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return result;
    }

    private void init() throws IOException {
        data = Data.create(datasetFullPath, StandardCharsets.UTF_8, datasetDelimiter);
        for (String att : identifyingAttributes) {
            data.getDefinition().setAttributeType(att, AttributeType.IDENTIFYING_ATTRIBUTE);
        }
        for (String att : insensitiveAttributes) {
            data.getDefinition().setAttributeType(att, AttributeType.INSENSITIVE_ATTRIBUTE);
        }
        for (String att : sensitiveAttributes) {
            data.getDefinition().setAttributeType(att, AttributeType.SENSITIVE_ATTRIBUTE);
        }
        for (int i = 0; i < quasiIdentifier.length; i++) {
            data.getDefinition().setAttributeType(quasiIdentifier[i], AttributeType.Hierarchy.create(quasiIdentifierHierarchyFullPath[i], StandardCharsets.UTF_8, hierarchyDelimiter));
        }
    }

    private void optimize(HashMap<String,HashMap<String,ArrayList<String>>> workloadUtility ){
        Optimizer optimizer = new Optimizer(config,data, quasiIdentifier, workloadUtility);
        optimizer.execute();
        //data = optimizer.getData();
        //qidOptimized = optimizer.getQidOptimized();
    }

    public boolean optimizedAnonymization(HashMap<String,HashMap<String,ArrayList<String>>> workloadUtility){
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        optimize(workloadUtility);
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        // Execute the algorithm

        try {
            data.getHandle().release();
            result = anonymizer.anonymize(data, config);
        } catch (IOException e) {
            e.printStackTrace();
            return  false;
        }
        return true;
    }

    public boolean anonymize(){
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        // Execute the algorithm
        try {
            data.getHandle().release();
            result = anonymizer.anonymize(data, config);
        } catch (IOException e) {
            e.printStackTrace();
            return  false;
        }
        return true;
    }

    public String getGeneralization(){
        // Extract
        final ARXLattice.ARXNode optimum = result.getGlobalOptimum();
        final List<String> qis = new ArrayList<String>(data.getDefinition().getQuasiIdentifyingAttributes());

        if (optimum == null) {
            return null;
        }

        // Initialize
        final StringBuffer[] identifiers = new StringBuffer[qis.size()];
        final StringBuffer[] generalizations = new StringBuffer[qis.size()];
        int lengthI = 0;
        int lengthG = 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < qis.size(); i++) {
            generalizations[i] = new StringBuffer();
            generalizations[i].append(optimum.getGeneralization(qis.get(i)));
            if (data.getDefinition().isHierarchyAvailable(qis.get(i)))
                generalizations[i].append("/").append(data.getDefinition().getHierarchy(qis.get(i))[0].length - 1);
        }
        for (int i = 0; i < qis.size(); i++) {
            sb.append(generalizations[i].toString().trim()).append(",");
        }
        String res = sb.toString();
        return res.substring(0, res.length()-1);
    }

    private void printQuality(ARXResult result, Data data, String fileName) throws IOException {

        // Extract
        final ARXLattice.ARXNode optimum = result.getGlobalOptimum();
        final List<String> qis = new ArrayList<String>(data.getDefinition().getQuasiIdentifyingAttributes());

        if (optimum == null) {
            return ;
        }

        // Initialize
        final StringBuffer[] identifiers = new StringBuffer[qis.size()];
        final StringBuffer[] generalizations = new StringBuffer[qis.size()];
        int lengthI = 0;
        int lengthG = 0;
        for (int i = 0; i < qis.size(); i++) {
            identifiers[i] = new StringBuffer();
            generalizations[i] = new StringBuffer();
            identifiers[i].append(qis.get(i));
            generalizations[i].append(optimum.getGeneralization(qis.get(i)));
            if (data.getDefinition().isHierarchyAvailable(qis.get(i)))
                generalizations[i].append("/").append(data.getDefinition().getHierarchy(qis.get(i))[0].length - 1);
            lengthI = Math.max(lengthI, identifiers[i].length());
            lengthG = Math.max(lengthG, generalizations[i].length());
        }

        // Padding
        for (int i = 0; i < qis.size(); i++) {
            while (identifiers[i].length() < lengthI) {
                identifiers[i].append(" ");
            }
            while (generalizations[i].length() < lengthG) {
                generalizations[i].insert(0, " ");
            }
        }

        File dir = new File("./results/quality");
        dir.mkdirs();
        File file = new File(dir,fileName);

        StatisticsQuality utility = result.getOutput(result.getGlobalOptimum(), false).getStatistics().getQualityStatistics();
        StringBuilder sb = new StringBuilder();
        if(!file.exists()) {
            sb.append("AnonType,");
            for (int i = 0; i < qis.size(); i++) {
                sb.append(identifiers[i].toString().trim()).append(",");
            }
            sb.append("DatasetAmbiguity,DatasetAverageClassSize,DatasetDiscernibility,DatasetRecordLevelSquareError,AttributeMeanGranularity,AttributeMeanSquareError,AttributeMeanNonUniformEntropy,AttributeMeanPrecision");
            for (int i = 0; i < qis.size(); i++) {
                sb.append(",");
                sb.append(identifiers[i].toString().trim()).append("_Precision");
            }
            for (int i = 0; i < qis.size(); i++) {
                sb.append(",");
                sb.append(identifiers[i].toString().trim()).append("_NonUniformEntropy");
            }
            sb.append("\n");
        }
        sb.append(fileName).append(",");
        for (int i = 0; i < qis.size(); i++) {
            sb.append(generalizations[i].toString().trim()).append(",");
        }
        sb.append(utility.getAmbiguity().getValue()).append(",").append(utility.getAverageClassSize().getValue()).append(",").append(utility.getDiscernibility().getValue()).append(",").append(utility.getRecordLevelSquaredError().getValue()).append(",");
        sb.append(utility.getGranularity().getArithmeticMean(false)).append(",").append(utility.getAttributeLevelSquaredError().getArithmeticMean(false)).append(",").append(utility.getNonUniformEntropy().getArithmeticMean(false)).append(",").append(utility.getGeneralizationIntensity().getArithmeticMean(false));

        QualityMeasureColumnOriented generalization = utility.getGeneralizationIntensity();
        for (int i = 0; i < qis.size(); i++) {
            sb.append(",");
            if (generalization.isAvailable(identifiers[i].toString().trim())){
                sb.append(generalization.getValue(identifiers[i].toString().trim()));
            } else {
                sb.append("0");
            }
        }


        QualityMeasureColumnOriented nonuniformentropy = utility.getNonUniformEntropy();
        for (int i = 0; i < qis.size(); i++) {
            sb.append(",");
            if (nonuniformentropy.isAvailable(identifiers[i].toString().trim())){
                sb.append(nonuniformentropy.getValue(identifiers[i].toString().trim()));
            } else {
                sb.append("0");
            }
        }
        sb.append("\n");

        FileOutputStream fileOutputStream = new FileOutputStream(file,true);

        fileOutputStream.write(sb.toString().getBytes());
    }


    public boolean saveToCSV(String directory, String fileName) {
        if (result.getGlobalOptimum()!=null) {
            File dir = new File(directory);
            dir.mkdirs();
            File file = new File(dir, fileName);
            try {
                result.getOutput(false).save(file);
                printQuality(result,data,fileName);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
        return false;
    }
}
