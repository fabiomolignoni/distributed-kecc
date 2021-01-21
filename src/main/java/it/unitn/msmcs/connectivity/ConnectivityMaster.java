package it.unitn.msmcs.connectivity;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

public class ConnectivityMaster extends DefaultMasterCompute {

    public static final String CONTINUE_COMPUTATION = "UPDATED-SUBGRAPH";
    public static final String K_ECC = "K-ECC";
    public static final String ROUND = "ROUND";
    public static final String NEED_CUT = "NEED_CUT";
    public static final String CONSTRAINED = "CONSTRAINED";
    public static final String FIRSTCUT = "FIRSTCUT";

    public static int NITERATIONS = 1;
    public static boolean DIRECTED = false;

    private Phase phase = Phase.INIT_COMPUTATION;
    private int k = 1;
    private int round = 0;
    private int currentIteration = 0;

    @Override
    public void compute() {
        setAggregatedValue(ROUND, new IntWritable(round));
        switch (phase) {
            case INIT_COMPUTATION:
                initComputation();
                break;
            case AGREEMENT:
                findAgreement();
                break;
            case CONTRACTION:
                contractEdges();
                break;
            case CONSTRAINT_CONTRACTION:
                constraintedContraction();
                break;
            case FIND_COMPONENTS:
                findConnectedComponents();
                break;
            case PERFORM_CUT:
                performCut();
                break;
            case RESTORE_GRAPH:
                restoreGraph();
                break;

        }
        // DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");
        // System.out.println("PHASE " + phase + " - " + dtf.format(LocalDateTime.now())
        // + " " + getTotalNumVertices());

    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        NITERATIONS = Integer.parseInt(getConf().get("input.iterations"));
        k = Integer.parseInt(getConf().get("input.k"));
        DIRECTED = getConf().get("input.directed") == "true";

        registerAggregator(CONTINUE_COMPUTATION, BooleanOrAggregator.class);
        registerAggregator(NEED_CUT, BooleanOrAggregator.class);
        registerAggregator(CONSTRAINED, BooleanOrAggregator.class);
        registerPersistentAggregator(FIRSTCUT, BooleanOrAggregator.class);
        registerPersistentAggregator(K_ECC, IntSumAggregator.class);
        registerPersistentAggregator(ROUND, IntSumAggregator.class);
    }

    private void initComputation() {
        if (getSuperstep() < 2) {
            setComputation(InitComputation.class);
        } else {

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");
            System.out.println("========= BEGIN " + k + " - " + dtf.format(LocalDateTime.now()));
            setAggregatedValue(FIRSTCUT, new BooleanWritable(true));
            setAggregatedValue(K_ECC, new IntWritable(k));
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.PERFORM_CUT;
            performCut();

        }
    }

    private void performCut() {
        BooleanWritable cont = getAggregatedValue(CONTINUE_COMPUTATION);
        if (round == 0 || round % 2 == 1 || cont.get()) {
            setComputation(PerformCut.class);
            round++;
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.AGREEMENT;
            findAgreement();
            setAggregatedValue(FIRSTCUT, new BooleanWritable(false));

        }
    }

    private void findAgreement() {
        BooleanWritable cont = getAggregatedValue(CONTINUE_COMPUTATION);
        if (round < 2 || cont.get()) {
            setComputation(FindAgreement.class);
            round++;
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.CONTRACTION;
            contractEdges();
        }
    }

    private void contractEdges() {
        if (round < 3) {
            setComputation(ContractEdges.class);
            round++;
        } else {
            BooleanWritable cont = getAggregatedValue(CONTINUE_COMPUTATION);
            BooleanWritable needCut = getAggregatedValue(NEED_CUT);
            BooleanWritable constrained = getAggregatedValue(CONSTRAINED);
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            if (constrained.get()) {
                phase = Phase.CONSTRAINT_CONTRACTION;
                constraintedContraction();
            } else if (needCut.get()) {
                phase = Phase.PERFORM_CUT;
                performCut();
            } else if (cont.get()) {
                phase = Phase.AGREEMENT;
                findAgreement();
            } else {

                phase = Phase.FIND_COMPONENTS;
                findConnectedComponents();
            }

        }
    }

    private void constraintedContraction() {
        BooleanWritable cont = getAggregatedValue(CONTINUE_COMPUTATION);
        if (round < 2 || cont.get()) {
            setComputation(ConstrainedContract.class);
            round++;
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.CONTRACTION;
            contractEdges();
        }
    }

    private void findConnectedComponents() {
        BooleanWritable cont = getAggregatedValue(CONTINUE_COMPUTATION);
        if (round < 2 || cont.get()) {
            setComputation(FindComponents.class);
            round++;
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.RESTORE_GRAPH;
            restoreGraph();

        }
    }

    private void restoreGraph() {
        if (round < 2) {
            setComputation(RestoreGraph.class);
            round++;
        } else {
            currentIteration += 1;
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");
            setAggregatedValue(FIRSTCUT, new BooleanWritable(true));
            System.out.println("========= END ITERATION " + currentIteration + " - " + dtf.format(LocalDateTime.now()));
            if (currentIteration == NITERATIONS) {
                haltComputation();
            }
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.PERFORM_CUT;
            performCut();

        }
    }

    private enum Phase {
        INIT_COMPUTATION, AGREEMENT, CONTRACTION, CONSTRAINT_CONTRACTION, FIND_COMPONENTS, PERFORM_CUT, RESTORE_GRAPH;
    }
}