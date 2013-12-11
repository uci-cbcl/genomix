package edu.uci.ics.genomix.mixture.model;

import java.io.IOException;
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

    public void drawHist(Map<Double, Long> binData, String histType) throws IOException {
        XYSeries series = new XYSeries(histType);
        for (Entry<Double, Long> pair : binData.entrySet()) {
            series.add(pair.getKey().doubleValue(), pair.getValue().longValue());
        }
        XYSeriesCollection xyDataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createXYBarChart("bin", "Normal", false, "Count", xyDataset,
                PlotOrientation.VERTICAL, true, true, false);

        // Write the data to the output stream:
        FileSystem fileSys = FileSystem.get(new Configuration());
        FSDataOutputStream outstream = fileSys.create(new Path(histType + "-hist.png"), true);
        ChartUtilities.writeChartAsPNG(outstream, chart, 800, 600);
        outstream.close();
    }

    public static void main(String[] args) throws IOException {
        FittingMixture fm = new FittingMixture();

        double expMean = 3;
        ExponentialDistribution expDist = new ExponentialDistribution(expMean);

        double[] exp_randomSample = expDist.sample(5000);
        Map<Double, Long> exp_histData = fm.calcHistogram(exp_randomSample, 0, 100, 100);
        fm.drawHist(exp_histData, "Exponential");

        double normMean = 50;
        double normStd = 10;
        NormalDistribution normDist = new NormalDistribution(normMean, normStd);

        double[] norm_randomSample = normDist.sample(5000);
        Map<Double, Long> norm_histData = fm.calcHistogram(norm_randomSample, 0, 100, 100);
        fm.drawHist(norm_histData, "Normal");
    }

}
