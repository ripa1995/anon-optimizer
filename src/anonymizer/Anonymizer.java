package anonymizer;

import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.StatisticsQuality;
import org.deidentifier.arx.aggregates.quality.QualityMeasureColumnOriented;
import org.deidentifier.arx.criteria.KAnonymity;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Anonymizer {
    private String datasetFullPath;
    private String[] sensitiveAttributes, quasiIdentifier, quasiIdentifierHierarchyFullPath, identifyingAttributes, insensitiveAttributes;
    private char datasetDelimiter, hierarchyDelimiter;
    private Data data;
    private boolean optimized;
    private ArrayList<String> qidOptimized;
    private int k;
    private double suppression;
    private ARXResult result;

    public Anonymizer(){
        this.datasetFullPath = ".\\data\\dataset\\adult_clear.csv";
        this.identifyingAttributes = new String[]{};
        this.sensitiveAttributes = new String[]{};
        this.insensitiveAttributes = new String[]{"fnlwgt","education-num","relationship","capital-gain","capital-loss","hours-per-week"};
        this.quasiIdentifier = new String[]{"age","workclass","education","marital-status","occupation","race","sex","native-country","salary-class"};
        this.quasiIdentifierHierarchyFullPath = new String[]{".\\data\\hierarchies\\adult_age.csv",
                ".\\data\\hierarchies\\adult_workclass.csv",
                ".\\data\\hierarchies\\adult_education.csv",
                ".\\data\\hierarchies\\adult_marital-status.csv",
                ".\\data\\hierarchies\\adult_occupation.csv",
                ".\\data\\hierarchies\\adult_race.csv",
                ".\\data\\hierarchies\\adult_sex.csv",
                ".\\data\\hierarchies\\adult_native-country.csv",
                ".\\data\\hierarchies\\adult_salary-class.csv"};
        this.datasetDelimiter = ',';
        this.hierarchyDelimiter = ';';
        this.data = null;
        this.optimized = false;
        this.qidOptimized = null;
        this.k = 5;
        this.suppression = 0.04;
        this.result = null;
    }

    public Anonymizer(int k, double suppression, String datasetFullPath, String[] identifyingAttributes, String[] sensitiveAttributes, String[] insensitiveAttributes, String[] quasiIdentifier, String[] quasiIdentifierHierarchyFullPath, char datasetDelimiter, char hierarchyDelimiter){
        this.datasetFullPath = datasetFullPath;
        this.identifyingAttributes = identifyingAttributes;
        this.sensitiveAttributes = sensitiveAttributes;
        this.insensitiveAttributes = insensitiveAttributes;
        this.quasiIdentifier = quasiIdentifier;
        this.quasiIdentifierHierarchyFullPath = quasiIdentifierHierarchyFullPath;
        this.datasetDelimiter = datasetDelimiter;
        this.hierarchyDelimiter = hierarchyDelimiter;
        this.data = null;
        this.optimized = false;
        this.qidOptimized = null;
        this.k = k;
        this.suppression = suppression;
        this.result = null;
    }

    public boolean init() throws IOException {
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void optimize(HashMap<String,HashMap<String,ArrayList<String>>> workloadUtility ){
        Optimizer optimizer = new Optimizer(k,suppression,data, quasiIdentifier, workloadUtility);
        optimizer.execute();
        //data = optimizer.getData();
        //qidOptimized = optimizer.getQidOptimized();
        optimized=true;
    }

    public boolean anonymize(){
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        // Execute the algorithm
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new KAnonymity(k));
        config.setSuppressionLimit(suppression);
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
