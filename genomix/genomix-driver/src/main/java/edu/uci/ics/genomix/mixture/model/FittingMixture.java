package edu.uci.ics.genomix.mixture.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class FittingMixture {

    public Map<Double, Long> calcHistogram(double[] data, double min, double max, int numBins) {
        final long[] result = new long[numBins];
        final double binSize = (max - min) / numBins;

        for (double d : data) {
            int bin = (int) ((d - min) / binSize);
            if (bin < 0) { /* this data is smaller than min */
            } else if (bin >= numBins) { /* this data point is bigger than max */
            } else {
                result[bin]++;
            }
        }

        Map<Double, Long> histData = new HashMap<Double, Long>();
        for (int i = 0; i < result.length; i++) {
            Double key = min + i * binSize;
            histData.put(key, result[i]);
        }

        return histData;
    }

    public static void drawHist(ArrayList<Double> array, String histType) throws IOException {
        XYSeries series = new XYSeries(histType);
        for (int i = 0; i < array.size(); i++) {
            series.add(i + 1, array.get(i));
        }
        XYSeriesCollection xyDataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createXYBarChart(histType, "bin", false, "Count", xyDataset,
                PlotOrientation.VERTICAL, true, true, false);

        // Write the data to the output stream:
        FileSystem fileSys = FileSystem.get(new Configuration());
        FSDataOutputStream outstream = fileSys.create(new Path(histType + "-hist.png"), true);
        ChartUtilities.writeChartAsPNG(outstream, chart, 800, 600);
        outstream.close();
    }

    public void drawHist(Map<Double, Long> binData, String histType) throws IOException {
        XYSeries series = new XYSeries(histType);
        for (Entry<Double, Long> pair : binData.entrySet()) {
            series.add(pair.getKey().doubleValue(), pair.getValue().longValue());
        }
        XYSeriesCollection xyDataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createXYBarChart(histType, "bin", false, "Count", xyDataset,
                PlotOrientation.VERTICAL, true, true, false);

        // Write the data to the output stream:
        FileSystem fileSys = FileSystem.get(new Configuration());
        FSDataOutputStream outstream = fileSys.create(new Path(histType + "-hist.png"), true);
        ChartUtilities.writeChartAsPNG(outstream, chart, 800, 600);
        outstream.close();
    }

    public double computeLikelihood(double[] data, double prob_Exp, double prob_Normal, double[] prob_expMembership,
            double[] prob_normalMembership) {
        double likelihood = 0;
        for (int i = 0; i < data.length; i++)
            likelihood += Math.log(prob_Exp * prob_expMembership[i] + prob_Normal * prob_normalMembership[i]);
        System.out.println(likelihood);
        return likelihood;
    }

    public static double fittingMixture(double[] data, double max, int numOfIterations) throws IOException {
        FittingMixture fm = new FittingMixture();
        ArrayList<Double> likelihoods = new ArrayList<Double>();

        /* initial guess at parameters */
        double cur_expMean = 5;
        ExponentialDistribution cur_expDist = new ExponentialDistribution(cur_expMean);

        double cur_normalMean = 20;
        double cur_normalStd = 5;
        NormalDistribution cur_normalDist = new NormalDistribution(cur_normalMean, cur_normalStd);

        /* probability of each data point belonging to each class */
        double prob_Exp = 0.5;
        double prob_Normal = 0.5;

        // initiate prob_membership of exp and normal
        double[] prob_expMembership = new double[data.length];
        double[] prob_normalMembership = new double[data.length];

        //        likelihoods.add(computeLikelihood(data, prob_Exp, prob_Normal, prob_expMembership, prob_normalMembership));
        /* Given a fixed set of parameters, it chooses the probability distribution 
         * which maximizes likelihood */
        for (int i = 0; i < data.length; i++) {
            // calculate the expectation prob of the two classes
            prob_expMembership[i] = cur_expDist.density(data[i]) * prob_Exp;
            if (prob_expMembership[i] == 0)
                prob_expMembership[i] = 0.000000001;
            prob_normalMembership[i] = cur_normalDist.density(data[i]) * prob_Normal;
            if (prob_normalMembership[i] == 0)
                prob_normalMembership[i] = 0.000000001;

            // normalize
            double membershipSum = prob_expMembership[i] + prob_normalMembership[i];
            prob_expMembership[i] = prob_expMembership[i] / membershipSum;
            prob_normalMembership[i] = prob_normalMembership[i] / membershipSum;
        }
        double exp_overallSum = 0;
        double normal_overallSum = 0;
        for (int i = 0; i < data.length; i++) {
            exp_overallSum += prob_expMembership[i];
            normal_overallSum += prob_normalMembership[i];
        }
        // normalize
        double overallSum = exp_overallSum + normal_overallSum;
        prob_Exp = exp_overallSum / overallSum;
        prob_Normal = normal_overallSum / overallSum;

        /* Given a fixed probability distribution, update the parameters
         * which maximize likelihood*/
        for (int n = 0; n < numOfIterations; n++) {
            // get the weighted means for each mean/std
            cur_expMean = 0;
            cur_normalMean = 0;
            cur_normalStd = 0;
            for (int i = 0; i < data.length; i++) {
                cur_expMean += prob_expMembership[i] * data[i];
                cur_normalMean += prob_normalMembership[i] * data[i];
            }
            cur_expMean = cur_expMean / exp_overallSum;
            cur_expDist = new ExponentialDistribution(cur_expMean);
            cur_normalMean = cur_normalMean / normal_overallSum;
            for (int i = 0; i < data.length; i++) {
                cur_normalStd += prob_normalMembership[i] * Math.pow(data[i] - cur_normalMean, 2);
            }

            cur_normalStd = Math.sqrt(cur_normalStd / normal_overallSum);
            if (cur_normalStd == 0)
                cur_normalStd = 0.000000001f;
            cur_normalDist = new NormalDistribution(cur_normalMean, cur_normalStd);

            /* Given a fixed set of parameters, it chooses the probability distribution 
             * which maximizes likelihood */
            for (int i = 0; i < data.length; i++) {
                // calculate the expectation prob of the two classes
                prob_expMembership[i] = cur_expDist.density(data[i]) * prob_Exp;
                if (prob_expMembership[i] == 0)
                    prob_expMembership[i] = 0.000000001;
                prob_normalMembership[i] = cur_normalDist.density(data[i]) * prob_Normal;
                if (prob_normalMembership[i] == 0)
                    prob_normalMembership[i] = 0.000000001;

                // normalize
                double membershipSum = prob_expMembership[i] + prob_normalMembership[i];
                prob_expMembership[i] = prob_expMembership[i] / membershipSum;
                prob_normalMembership[i] = prob_normalMembership[i] / membershipSum;
            }
            exp_overallSum = 0;
            normal_overallSum = 0;
            for (int i = 0; i < data.length; i++) {
                exp_overallSum += prob_expMembership[i];
                normal_overallSum += prob_normalMembership[i];
            }
            // normalize
            overallSum = exp_overallSum + normal_overallSum;
            prob_Exp = exp_overallSum / overallSum;
            prob_Normal = normal_overallSum / overallSum;

            likelihoods.add(fm
                    .computeLikelihood(data, prob_Exp, prob_Normal, prob_expMembership, prob_normalMembership));
        }

        /* For test */
        //        double[] exp_randomSample = cur_expDist.sample(5000);
        //        Map<Double, Long> exp_histData = fm.calcHistogram(exp_randomSample, 0, max, (int) (max));
        //        fm.drawHist(exp_histData, "After-Exponential");
        //
        //        double[] norm_randomSample = cur_normalDist.sample(5000);
        //        Map<Double, Long> norm_histData = fm.calcHistogram(norm_randomSample, 0, max, (int) (max));
        //        fm.drawHist(norm_histData, "After-Normal");
        //
        //        double[] combine_randomSample = fm.combineTwoDoubleArray(exp_randomSample, norm_randomSample);
        //        Map<Double, Long> combine_histData = fm.calcHistogram(combine_randomSample, 0, max, (int) (max));
        //        fm.drawHist(combine_histData, "Mixture");
        //
        //        // plot likelihood's change
        //        fm.drawHist(likelihoods, "Likelihood");

        /* one choice of threshold is when the probability of belonging to the normal distribution 
         is greater than the probability of belonging to the exponential distribution
         this happens when the ratio of norm to exponential is > 1 */
        double prob_in_exp = 0;
        double prob_in_normal = 0;
        for (double cov = 1; cov < max; cov++) {
            prob_in_exp = prob_Exp * cur_expDist.density(cov);
            prob_in_normal = prob_Normal * cur_normalDist.density(cov);
            if (prob_in_exp < prob_in_normal)
                return cov;
        }
        return 0;
    }

    public double[] combineTwoDoubleArray(double[] array1, double[] array2) {
        double[] array1and2 = new double[array1.length + array2.length];
        System.arraycopy(array1, 0, array1and2, 0, array1.length);
        System.arraycopy(array2, 0, array1and2, array1.length, array2.length);
        return array1and2;
    }

    public static void main(String[] args) throws IOException {
        FittingMixture fm = new FittingMixture();

        double expMean = 3;
        ExponentialDistribution expDist = new ExponentialDistribution(expMean);

        double[] exp_randomSample = expDist.sample(5000);
        Map<Double, Long> exp_histData = fm.calcHistogram(exp_randomSample, 0, 100, 100);
        fm.drawHist(exp_histData, "Before-Exponential");

        double normMean = 50;
        double normStd = 10;
        NormalDistribution normDist = new NormalDistribution(normMean, normStd);

        double[] norm_randomSample = normDist.sample(5000);
        Map<Double, Long> norm_histData = fm.calcHistogram(norm_randomSample, 0, 100, 100);
        fm.drawHist(norm_histData, "Before-Normal");

        double[] combine_randomSample = fm.combineTwoDoubleArray(exp_randomSample, norm_randomSample);
        Map<Double, Long> combine_histData = fm.calcHistogram(combine_randomSample, 0, 100, 100);
        fm.drawHist(combine_histData, "Mixture");

        //        fm.fittingMixture(combine_randomSample, 10);
    }

}
