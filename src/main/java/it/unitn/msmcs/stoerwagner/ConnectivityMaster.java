package it.unitn.msmcs.stoerwagner;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import it.unitn.msmcs.common.aggregators.SubgraphInfoAggregator;

public class ConnectivityMaster extends DefaultMasterCompute {

    public static final String CONTINUE_COMPUTATION = "UPDATED-SUBGRAPH";
    public static final String SUBGRAPH_INFO = "SUBGRAPH_INFO";
    public static final String K = "K-ECC";
    public static final String ROUND = "ROUND";
    public static final String REMAINING = "REMAINING";

    private Phase phase = Phase.INIT_COMPUTATION;

    int k = 1;
    int round = 0;

    @Override
    public void compute() {
        setAggregatedValue(ROUND, new IntWritable(round));
        switch (phase) {
            case INIT_COMPUTATION:
                initComputation();
                break;
            case INIT_SUBGRAPHS:
                initSubgraph();
                break;
            case INIT_STOER_WAGNER:
                initStoerWagner();
                break;
            case COMPUTE_STOER_WAGNER:
                computeStoerWagner();
                break;
            case CONTRACT_SUBGRAPH:
                contractSubgraph();
                break;

        }
    }

    private void initComputation() {
        if (getSuperstep() < 2) {
            setComputation(InitComputation.class);
        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.INIT_SUBGRAPHS;
            initSubgraph();
        }
    }

    private void initSubgraph() {
        BooleanWritable updated = getAggregatedValue(CONTINUE_COMPUTATION);
        if (round == 0 || updated.get()) {
            setComputation(InitSubgraphs.class);
            round += 1;

        } else { // If none updated its value go to the next phase
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.INIT_STOER_WAGNER;
            initStoerWagner();

        }
    }

    private void initStoerWagner() {
        BooleanWritable continueComputation = getAggregatedValue(CONTINUE_COMPUTATION);
        if (round == 0) {
            setAggregatedValue(K, new IntWritable(k)); // Communicate K
            setComputation(InitStoerWagner.class);
            round += 1;
        } else if (continueComputation.get()) {
            IntWritable remaining = getAggregatedValue(REMAINING);
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
            System.out.println(
                    remaining + " vertices remaining - " + dtf.format(LocalDateTime.now()) + " - " + getSuperstep());

            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.COMPUTE_STOER_WAGNER;
            computeStoerWagner();
        } else {
            IntWritable remaining = getAggregatedValue(REMAINING);
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
            System.out.println(remaining + " subgraphs - " + dtf.format(LocalDateTime.now()) + " - " + getSuperstep());
            haltComputation();
        }

    }

    private void computeStoerWagner() {
        BooleanWritable continueComputation = getAggregatedValue(CONTINUE_COMPUTATION);
        if (round % 3 != 1 || continueComputation.get()) {
            setComputation(StoerWagner.class);
            round = (round + 1) % 3;

        } else {
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            phase = Phase.CONTRACT_SUBGRAPH;
            contractSubgraph();
        }
    }

    private void contractSubgraph() {
        if (round < 4) {
            setComputation(ContractGraph.class);
            round += 1;
        } else {
            phase = Phase.INIT_SUBGRAPHS;
            round = 0;
            setAggregatedValue(ROUND, new IntWritable(round));
            initSubgraph();
        }
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        k = Integer.parseInt(getConf().get("input.k"));
        registerAggregator(CONTINUE_COMPUTATION, BooleanOrAggregator.class);
        registerAggregator(SUBGRAPH_INFO, SubgraphInfoAggregator.class);
        registerAggregator(REMAINING, IntSumAggregator.class);
        registerPersistentAggregator(K, IntSumAggregator.class);
        registerPersistentAggregator(ROUND, IntSumAggregator.class);
    }

    private enum Phase {
        INIT_COMPUTATION, INIT_SUBGRAPHS, INIT_STOER_WAGNER, COMPUTE_STOER_WAGNER, CONTRACT_SUBGRAPH;
    }
}