package it.unitn.msmcs.kecc;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

public class ConnectivityMaster extends DefaultMasterCompute {

    public static String ROUND = "ROUND"; // round aggregator
    public static String CONTINUE = "CONTINUE"; // aggregator true if we should continue the computation
    public static String K = "K"; // aggregator for k value of k-ECC
    public static String CUT = "CUT"; // aggregator true if graph needs a cut
    public static String FORCED = "FORCED"; // aggregator true if graph needs a cut

    private int nIterations = 1; // number of iterations to perform
    private int iterationsDone = 0; // number of iterations already done
    private int k = 2; // value of k-ECC
    private int round = 0; // current round
    private Phase phase = Phase.INIT_COMPUTATION; // current function to perform

    @Override
    public void compute() {
        setAggregatedValue(ROUND, new IntWritable(round));

        // Decide which algorithm to execute
        switch (phase) {
            case INIT_COMPUTATION:
                initComputation();
                break;
            case PERFORM_CUTS:
                performCuts();
                break;
            case RANDOM_AGREE:
                findRandomAgreements();
                break;
            case CONTRACT_GRAPH:
                contractGraph();
                break;
            case FORCED_AGREEMENTS:
                computeForcedAgreements();
                break;
            case WAKEUP:
                wakeUp();
                break;
            case FIND_COMPONENTS:
                findComponents();
                break;
            case RESTORE_GRAPH:
                restoreGraph();
                break;
        }
        // DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");
        // System.out.println("PHASE " + phase + " - " + dtf.format(LocalDateTime.now())
        // + " " + getTotalNumVertices());

    }

    /**
     * Initialize the relevant variables for the computation.
     */
    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {

        // Read user inputs
        nIterations = Integer.parseInt(getConf().get("input.iterations"));
        k = Integer.parseInt(getConf().get("input.k"));

        // Initialize aggregators
        registerAggregator(ROUND, IntSumAggregator.class);
        registerAggregator(CONTINUE, BooleanOrAggregator.class);
        registerAggregator(CUT, BooleanOrAggregator.class);
        registerAggregator(FORCED, BooleanOrAggregator.class);
        registerPersistentAggregator(K, IntSumAggregator.class);
    }

    /**
     * Trasform undirected graphs into directed.
     */
    private void initComputation() {
        if (getSuperstep() < 2) {
            setComputation(InitComputation.class);
        } else {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");
            System.out.println("========= BEGIN " + k + " - " + dtf.format(LocalDateTime.now()));
            // Set aggregated k value
            setAggregatedValue(K, new IntWritable(k));
            // Perform cuts
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.PERFORM_CUTS;
            performCuts();
        }
    }

    /**
     * Perform cuts for vertices with degree smaller than k.
     */
    private void performCuts() {
        BooleanWritable cont = getAggregatedValue(CONTINUE);
        if (round == 0 || round % 2 == 1 || cont.get()) {
            setComputation(PerformCuts.class);
            round++;
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.RANDOM_AGREE;
            findRandomAgreements();
        }

    }

    /**
     * Search for random agreements.
     */
    private void findRandomAgreements() {
        BooleanWritable cont = getAggregatedValue(CONTINUE);
        if (round < 2 || cont.get()) {
            setComputation(FindAgreements.class);
            round++;
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.CONTRACT_GRAPH;
            contractGraph();
        }
    }

    private void contractGraph() {
        if (round < 3) {
            setComputation(ContractGraph.class);
            round++;
        } else {
            BooleanWritable cont = getAggregatedValue(CONTINUE);
            BooleanWritable cut = getAggregatedValue(CUT);
            BooleanWritable forced = getAggregatedValue(FORCED);

            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            // note: priority to constrianed agree
            if (forced.get()) {
                phase = Phase.FORCED_AGREEMENTS;
                computeForcedAgreements();
            } else if (cut.get()) {
                phase = Phase.PERFORM_CUTS;
                performCuts();
            } else if (cont.get()) {
                phase = Phase.RANDOM_AGREE;
                findRandomAgreements();
            } else {
                phase = Phase.WAKEUP;
                wakeUp();
            }
        }
    }

    private void computeForcedAgreements() {
        BooleanWritable cont = getAggregatedValue(CONTINUE);
        if (round == 0 || cont.get()) {
            setComputation(ForcedAgreements.class);
            round++;
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.CONTRACT_GRAPH;
            contractGraph();
        }
    }

    private void wakeUp() {
        BooleanWritable cont = getAggregatedValue(CONTINUE);
        if (round == 0 || cont.get()) {
            setComputation(WakeUp.class);
            round++;
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.FIND_COMPONENTS;
            findComponents();
        }
    }

    private void findComponents() {
        BooleanWritable cont = getAggregatedValue(CONTINUE);
        if (round < 3 || cont.get()) {
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
            iterationsDone++;
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");
            System.out.println("========= END ITERATION " + iterationsDone + " - " + dtf.format(LocalDateTime.now()));
            if (iterationsDone < nIterations) {
                round = 0;
                setAggregatedValue(ROUND, new IntWritable(round));
                phase = Phase.PERFORM_CUTS;
                performCuts();
            } else {
                haltComputation();
            }
        }
    }

    private enum Phase {
        INIT_COMPUTATION, PERFORM_CUTS, RANDOM_AGREE, CONTRACT_GRAPH, FORCED_AGREEMENTS, WAKEUP, FIND_COMPONENTS,
        RESTORE_GRAPH;
    }
}