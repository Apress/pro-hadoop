package com.apress.hadoopbook.examples.ch8;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.MainProgrameShell;
import com.apress.hadoopbook.utils.Utils;

/** Demonstrate the use of MapSide joins
 *
 * There are three default data sets used, in the conf directory, maptest_a.txt, maptest_b.txt and maptest_c.txt.
 * The record keys in the file are some combination of {a0|a1|a2}:{b0|b1|b2}:{c0|c1|c2}.
 * All permutations of those are generated, these are available in maptest_all.txt
 *
 * For each key in maptest_all it is written to the a, b, c file, the applicable count from the key, times.
 *
 * For a key of the form a0:b1:c2, i will be present 0 times to maptest_a.txt, 1 time toin maptest_b.txt, and 2 times in maptest_c.txt.
 * 
 * The value provides information about the file, file line number, the ordinal number in this keys set, this line is, and the number of keys in this set.
 *
 * For a key a0:b1:c2, in maptest_c.txt, the key will be written twice, 2 being the number of keys in the set. the first time the ordinal number will be 1, the second time 2.
 *
 *
 * The output of the inner and outer joins will be key TAB mapset_a value TAB mapset_b value TAB mapset_c value TAB [X of Y] where X is the index of this value for the key, and Y is the number of values for the key.
 *
 * In the outer join any given mapset_? value may be missing, in the inner all will be populated.
 *
 * The override join will only have 1 value per key, and it will be the value from right most mapset file that had the key, in our case the right most file is mapset_c.txt
 *
 *
 * <ul>
 * <li> maptest_a.txt
 * <table border=0 cellpadding=0 cellspacing=0>
 *  <tr>
 *   <th>Key</th>
 *   <th>File</th>
 *   <th>Line Number</th>
 *   <th>Key # in Key Set</th>
 *   <th>Key Count in this file</th>
 *   <th>Key Sequence Number</th>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c0</td>
 *   <td>a</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>9</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c1</td>
 *   <td>a</td>
 *   <td>2</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>10</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c2</td>
 *   <td>a</td>
 *   <td>3</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>11</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c0</td>
 *   <td>a</td>
 *   <td>4</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>12</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c1</td>
 *   <td>a</td>
 *   <td>5</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>a</td>
 *   <td>6</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c0</td>
 *   <td>a</td>
 *   <td>7</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>15</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>a</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c0</td>
 *   <td>a</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>18</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c0</td>
 *   <td>a</td>
 *   <td>11</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>18</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c1</td>
 *   <td>a</td>
 *   <td>12</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>19</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c1</td>
 *   <td>a</td>
 *   <td>13</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>19</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>a</td>
 *   <td>14</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>20</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>a</td>
 *   <td>15</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>20</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c0</td>
 *   <td>a</td>
 *   <td>16</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>21</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c0</td>
 *   <td>a</td>
 *   <td>17</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>21</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>a</td>
 *   <td>18</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>22</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>a</td>
 *   <td>19</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>22</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>a</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>24</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>a</td>
 *   <td>23</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>24</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *  </tr>
 * </table>
 * </li>
 * <li>maptest_b.txt
 * <table border=0 cellpadding=0 cellspacing=0>
 *  <tr>
 *   <th>Key</th>
 *   <th>File</th>
 *   <th>Line Number</th>
 *   <th>Key # in Key Set</th>
 *   <th>Key Count in this file</th>
 *   <th>Key Sequence Number</th>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c0</td>
 *   <td>b</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>3</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c1</td>
 *   <td>b</td>
 *   <td>2</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>4</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c2</td>
 *   <td>b</td>
 *   <td>3</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>5</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c0</td>
 *   <td>b</td>
 *   <td>4</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>6</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c0</td>
 *   <td>b</td>
 *   <td>5</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>6</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c1</td>
 *   <td>b</td>
 *   <td>6</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>7</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c1</td>
 *   <td>b</td>
 *   <td>7</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>7</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td>b</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>8</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td>b</td>
 *   <td>9</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>8</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c0</td>
 *   <td>b</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>12</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c1</td>
 *   <td>b</td>
 *   <td>11</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>b</td>
 *   <td>12</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c0</td>
 *   <td>b</td>
 *   <td>13</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>15</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c0</td>
 *   <td>b</td>
 *   <td>14</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>15</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>b</td>
 *   <td>15</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>16</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>b</td>
 *   <td>16</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>16</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>b</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>b</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c0</td>
 *   <td>b</td>
 *   <td>19</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>21</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>b</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>b</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>24</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>b</td>
 *   <td>23</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>24</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>b</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>b</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *  </tr>
 * </table>
 * </li>
 * <li>maptest_c.txt
 * <table border=0 cellpadding=0 cellspacing=0>
 *  <tr>
 *   <th>Key</th>
 *   <th>File</th>
 *   <th>Line Number</th>
 *   <th>Key # in Key Set</th>
 *   <th>Key Count in this file</th>
 *   <th>Key Sequence Number</th>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c1</td>
 *   <td>c</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c2</td>
 *   <td>c</td>
 *   <td>2</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>2</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c2</td>
 *   <td>c</td>
 *   <td>3</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>2</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c1</td>
 *   <td>c</td>
 *   <td>4</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>4</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c2</td>
 *   <td>c</td>
 *   <td>5</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>5</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c2</td>
 *   <td>c</td>
 *   <td>6</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>5</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c1</td>
 *   <td>c</td>
 *   <td>7</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>7</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td>c</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>8</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td>c</td>
 *   <td>9</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>8</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c1</td>
 *   <td>c</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>10</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c2</td>
 *   <td>c</td>
 *   <td>11</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>11</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c2</td>
 *   <td>c</td>
 *   <td>12</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>11</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c1</td>
 *   <td>c</td>
 *   <td>13</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>c</td>
 *   <td>14</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>14</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>c</td>
 *   <td>15</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>14</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>c</td>
 *   <td>16</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>c</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>c</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c1</td>
 *   <td>c</td>
 *   <td>19</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>19</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>c</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>20</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>c</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>20</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>c</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>c</td>
 *   <td>23</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>c</td>
 *   <td>24</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *  </tr>
 * </table>
 * </li>
 * <table border=0 cellpadding=0 cellspacing=0>
 *  <tr>
 *   <th>Sequence Number</th>
 *   <th>Key</th>
 *  </tr>
 *  <tr>
 *   <td>1</td>
 *   <td>a0:b0:c1</td>
 *  </tr>
 *  <tr>
 *   <td>2</td>
 *   <td>a0:b0:c2</td>
 *  </tr>
 *  <tr>
 *   <td>1</td>
 *   <td>a0:b1:c0</td>
 *  </tr>
 *  <tr>
 *   <td>2</td>
 *   <td>a0:b1:c1</td>
 *  </tr>
 *  <tr>
 *   <td>3</td>
 *   <td>a0:b1:c2</td>
 *  </tr>
 *  <tr>
 *   <td>2</td>
 *   <td>a0:b2:c0</td>
 *  </tr>
 *  <tr>
 *   <td>3</td>
 *   <td>a0:b2:c1</td>
 *  </tr>
 *  <tr>
 *   <td>4</td>
 *   <td>a0:b2:c2</td>
 *  </tr>
 *  <tr>
 *   <td>1</td>
 *   <td>a1:b0:c0</td>
 *  </tr>
 *  <tr>
 *   <td>2</td>
 *   <td>a1:b0:c1</td>
 *  </tr>
 *  <tr>
 *   <td>3</td>
 *   <td>a1:b0:c2</td>
 *  </tr>
 *  <tr>
 *   <td>2</td>
 *   <td>a1:b1:c0</td>
 *  </tr>
 *  <tr>
 *   <td>3</td>
 *   <td>a1:b1:c1</td>
 *  </tr>
 *  <tr>
 *   <td>4</td>
 *   <td>a1:b1:c2</td>
 *  </tr>
 *  <tr>
 *   <td>3</td>
 *   <td>a1:b2:c0</td>
 *  </tr>
 *  <tr>
 *   <td>4</td>
 *   <td>a1:b2:c1</td>
 *  </tr>
 *  <tr>
 *   <td>5</td>
 *   <td>a1:b2:c2</td>
 *  </tr>
 *  <tr>
 *   <td>2</td>
 *   <td>a2:b0:c0</td>
 *  </tr>
 *  <tr>
 *   <td>3</td>
 *   <td>a2:b0:c1</td>
 *  </tr>
 *  <tr>
 *   <td>4</td>
 *   <td>a2:b0:c2</td>
 *  </tr>
 *  <tr>
 *   <td>3</td>
 *   <td>a2:b1:c0</td>
 *  </tr>
 *  <tr>
 *   <td>4</td>
 *   <td>a2:b1:c1</td>
 *  </tr>
 *  <tr>
 *   <td>5</td>
 *   <td>a2:b1:c2</td>
 *  </tr>
 *  <tr>
 *   <td>4</td>
 *   <td>a2:b2:c0</td>
 *  </tr>
 *  <tr>
 *   <td>5</td>
 *   <td>a2:b2:c1</td>
 *  </tr>
 *  <tr>
 *   <td>6</td>
 *   <td>a2:b2:c2</td>
 *  </tr>
 * </table>
 * </li>
 * </ul>
 *
 * The join results:
 * <ul>
 * <li> inner(tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_a.txt"),tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_b.txt"),tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_c.txt"))
 * <table border=0 cellpadding=0 cellspacing=0>
 *  <tr>
 *   <th>Key</th>
 *   <th>File A</th>
 *   <th>A Line</th>
 *   <th>A Repeat#</th>
 *   <th>A Repeat Count</th>
 *   <th>Sequence</th>
 *   <th>File B</th>
 *   <th>B Line</th>
 *   <th>B Repeat #</th>
 *   <th>B Repeat Count</th>
 *   <th>Sequence</th>
 *   <th>File C</th>
 *   <th>Line</th>
 *   <th>C Repeat #</th>
 *   <th>C Repeat Count</th>
 *   <th>Sequence</th>
 *   <th>Key Repeat Count</th>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c1</td>
 *   <td>a</td>
 *   <td>5</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *   <td>b</td>
 *   <td>11</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *   <td>c</td>
 *   <td>13</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *   <td></td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>a</td>
 *   <td>6</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *   <td>b</td>
 *   <td>12</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *   <td>c</td>
 *   <td>14</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>14</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>a</td>
 *   <td>6</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *   <td>b</td>
 *   <td>12</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *   <td>c</td>
 *   <td>15</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>14</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>a</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *   <td>b</td>
 *   <td>15</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>16</td>
 *   <td>c</td>
 *   <td>16</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>a</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *   <td>b</td>
 *   <td>16</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>16</td>
 *   <td>c</td>
 *   <td>16</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *   <td>b</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>c</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *   <td>b</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>c</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *   <td>b</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>c</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *   <td>b</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>c</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>a</td>
 *   <td>18</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>22</td>
 *   <td>b</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *   <td>c</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>a</td>
 *   <td>19</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>22</td>
 *   <td>b</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *   <td>c</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *   <td>c</td>
 *   <td>23</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *   <td>c</td>
 *   <td>23</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *   <td>c</td>
 *   <td>24</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *   <td>c</td>
 *   <td>24</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>b</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>b</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>b</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>b</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[1 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[2 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[3 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[4 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[5 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[6 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[7 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[8 of 8]</td>
 *  </tr>
 * </table>
 * </li>
 * <li> outer(tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_a.txt"),tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_b.txt"),tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_c.txt"))
 * <table border=0 cellpadding=0 cellspacing=0>
 *   <th>Key</th>
 *   <th>File A</th>
 *   <th>A Line</th>
 *   <th>A Repeat#</th>
 *   <th>A Repeat Count</th>
 *   <th>Sequence</th>
 *   <th>File B</th>
 *   <th>B Line</th>
 *   <th>B Repeat #</th>
 *   <th>B Repeat Count</th>
 *   <th>Sequence</th>
 *   <th>File C</th>
 *   <th>Line</th>
 *   <th>C Repeat #</th>
 *   <th>C Repeat Count</th>
 *   <th>Sequence</th>
 *   <th>Key Repeat Count</th>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c1</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td></td>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c2</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>2</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c2</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>3</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c0</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>3</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c1</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>2</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>4</td>
 *   <td>c</td>
 *   <td>4</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>4</td>
 *   <td></td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c2</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>3</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>5</td>
 *   <td>c</td>
 *   <td>5</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>5</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c2</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>3</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>5</td>
 *   <td>c</td>
 *   <td>6</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>5</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c0</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>4</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>6</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c0</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>5</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>6</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c1</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>6</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>7</td>
 *   <td>c</td>
 *   <td>7</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>7</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c1</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>7</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>7</td>
 *   <td>c</td>
 *   <td>7</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>7</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>8</td>
 *   <td>c</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>8</td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>8</td>
 *   <td>c</td>
 *   <td>9</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>8</td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>9</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>8</td>
 *   <td>c</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>8</td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>b</td>
 *   <td>9</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>8</td>
 *   <td>c</td>
 *   <td>9</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>8</td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c0</td>
 *   <td>a</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>9</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c1</td>
 *   <td>a</td>
 *   <td>2</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>10</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>10</td>
 *   <td></td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c2</td>
 *   <td>a</td>
 *   <td>3</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>11</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>11</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>11</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c2</td>
 *   <td>a</td>
 *   <td>3</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>11</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>12</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>11</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c0</td>
 *   <td>a</td>
 *   <td>4</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>12</td>
 *   <td>b</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>12</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c1</td>
 *   <td>a</td>
 *   <td>5</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *   <td>b</td>
 *   <td>11</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *   <td>c</td>
 *   <td>13</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *   <td></td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>a</td>
 *   <td>6</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *   <td>b</td>
 *   <td>12</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *   <td>c</td>
 *   <td>14</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>14</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>a</td>
 *   <td>6</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *   <td>b</td>
 *   <td>12</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>14</td>
 *   <td>c</td>
 *   <td>15</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>14</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c0</td>
 *   <td>a</td>
 *   <td>7</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>15</td>
 *   <td>b</td>
 *   <td>13</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>15</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c0</td>
 *   <td>a</td>
 *   <td>7</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>15</td>
 *   <td>b</td>
 *   <td>14</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>15</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>a</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *   <td>b</td>
 *   <td>15</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>16</td>
 *   <td>c</td>
 *   <td>16</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>a</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *   <td>b</td>
 *   <td>16</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>16</td>
 *   <td>c</td>
 *   <td>16</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *   <td>b</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>c</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *   <td>b</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>c</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *   <td>b</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>c</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>a</td>
 *   <td>9</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>17</td>
 *   <td>b</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>c</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c0</td>
 *   <td>a</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>18</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c0</td>
 *   <td>a</td>
 *   <td>11</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>18</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c1</td>
 *   <td>a</td>
 *   <td>12</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>19</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>19</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>19</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c1</td>
 *   <td>a</td>
 *   <td>13</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>19</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>19</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>19</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>a</td>
 *   <td>14</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>20</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>20</td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>a</td>
 *   <td>14</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>20</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>20</td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>a</td>
 *   <td>15</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>20</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>20</td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>a</td>
 *   <td>15</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>20</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>c</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>20</td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c0</td>
 *   <td>a</td>
 *   <td>17</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>21</td>
 *   <td>b</td>
 *   <td>19</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>21</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c0</td>
 *   <td>a</td>
 *   <td>16</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>21</td>
 *   <td>b</td>
 *   <td>19</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>21</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>a</td>
 *   <td>18</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>22</td>
 *   <td>b</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *   <td>c</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *   <td>[1 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>a</td>
 *   <td>19</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>22</td>
 *   <td>b</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *   <td>c</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *   <td>[2 of 2]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *   <td>c</td>
 *   <td>23</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *   <td>c</td>
 *   <td>24</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *   <td>c</td>
 *   <td>23</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>a</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>b</td>
 *   <td>21</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>23</td>
 *   <td>c</td>
 *   <td>24</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>a</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>24</td>
 *   <td>b</td>
 *   <td>23</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>24</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>a</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>24</td>
 *   <td>b</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>24</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>a</td>
 *   <td>23</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>24</td>
 *   <td>b</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>24</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>a</td>
 *   <td>23</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>24</td>
 *   <td>b</td>
 *   <td>23</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>24</td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td></td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>b</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *   <td>[1 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>b</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *   <td>[2 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>b</td>
 *   <td>24</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *   <td>[3 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>a</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>b</td>
 *   <td>25</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>25</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *   <td>[4 of 4]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[1 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[2 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[3 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[4 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[5 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[6 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[7 of 8]</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>a</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>b</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *   <td>[8 of 8]</td>
 *  </tr>
 * </table>
 * </li>
 * <li> override(tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_a.txt"),tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_b.txt"),tbl(org.apache.hadoop.mapred.KeyValueTextInputFormat,"maptest_c.txt"))
 * <table border=0 cellpadding=0 cellspacing=0>
 *  <tr>
 *   <td>Key</td>
 *   <td>File</td>
 *   <td>Line</td>
 *   <td>Repeat #</td>
 *   <td>Repeat Count</td>
 *   <td>Sequence</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c1</td>
 *   <td>c</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c2</td>
 *   <td>c</td>
 *   <td>2</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>2</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b0:c2</td>
 *   <td>c</td>
 *   <td>3</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>2</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c0</td>
 *   <td>b</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>3</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c1</td>
 *   <td>c</td>
 *   <td>4</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>4</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c2</td>
 *   <td>c</td>
 *   <td>5</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>5</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b1:c2</td>
 *   <td>c</td>
 *   <td>6</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>5</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c0</td>
 *   <td>b</td>
 *   <td>4</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>6</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c0</td>
 *   <td>b</td>
 *   <td>5</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>6</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c1</td>
 *   <td>c</td>
 *   <td>7</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>7</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td>c</td>
 *   <td>8</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>8</td>
 *  </tr>
 *  <tr>
 *   <td>a0:b2:c2</td>
 *   <td>c</td>
 *   <td>9</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>8</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c0</td>
 *   <td>a</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>9</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c1</td>
 *   <td>c</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>10</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c2</td>
 *   <td>c</td>
 *   <td>11</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>11</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b0:c2</td>
 *   <td>c</td>
 *   <td>12</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>11</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c0</td>
 *   <td>b</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>12</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c1</td>
 *   <td>c</td>
 *   <td>13</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>13</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>c</td>
 *   <td>15</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>14</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b1:c2</td>
 *   <td>c</td>
 *   <td>14</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>14</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c0</td>
 *   <td>b</td>
 *   <td>14</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>15</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c0</td>
 *   <td>b</td>
 *   <td>13</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>15</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c1</td>
 *   <td>c</td>
 *   <td>16</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>16</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>c</td>
 *   <td>17</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>17</td>
 *  </tr>
 *  <tr>
 *   <td>a1:b2:c2</td>
 *   <td>c</td>
 *   <td>18</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>17</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c0</td>
 *   <td>a</td>
 *   <td>10</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>18</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c0</td>
 *   <td>a</td>
 *   <td>11</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>18</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c1</td>
 *   <td>c</td>
 *   <td>19</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>19</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>c</td>
 *   <td>21</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>20</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b0:c2</td>
 *   <td>c</td>
 *   <td>20</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>20</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c0</td>
 *   <td>b</td>
 *   <td>19</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>21</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c1</td>
 *   <td>c</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>22</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>c</td>
 *   <td>23</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>23</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b1:c2</td>
 *   <td>c</td>
 *   <td>24</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>23</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>b</td>
 *   <td>22</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>24</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c0</td>
 *   <td>b</td>
 *   <td>23</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>24</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c1</td>
 *   <td>c</td>
 *   <td>25</td>
 *   <td>1</td>
 *   <td>1</td>
 *   <td>25</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>c</td>
 *   <td>26</td>
 *   <td>1</td>
 *   <td>2</td>
 *   <td>26</td>
 *  </tr>
 *  <tr>
 *   <td>a2:b2:c2</td>
 *   <td>c</td>
 *   <td>27</td>
 *   <td>2</td>
 *   <td>2</td>
 *   <td>26</td>
 *  </tr>
 * </table>
 * </li>
 * </ul>
 * 
 * @author Jason
 *
 */
public class MapSideJoinExample extends MainProgrameShell {
	/** general purpose logging. */
	static Logger LOG = Logger.getLogger(MapSideJoinExample.class);

	/** If true use the data sets on the command line, not the default data set. -nDD flag to toggle.
	 * If a data set has a :string, string is considered the input format. if string has no package components org.apache.hadoop.mapred is assumed.
	 * if :string is absent, {@link KeyValueTextInputFormat} is assumed.
	 */
	boolean noDefaultDatasets = false;
	
	/** If true, it is okay to delete input files that match the default data set names when creating the input data sets. -dDI to toggle. */
	boolean doDeleteInput = false;
	
	/** The resource names to use to generate the data sets for this test. */
	static String[] defaultDatasetResourceNames = new String[] { "maptest_a.txt", "maptest_b.txt", "maptest_c.txt" };

	/** The join types this example works for. */
	static String[] joinTypes = new String[] { "inner", "outer", "override" };
	
	/** The set of data sets to use for the joins. */
	ArrayList<String> datasets = new ArrayList<String>();

	  /** Add the --noDefaultDatasets and --noDeleteInput flags.
	   * 
	   * @see MainProgrameShell#buildGeneralOptions(Options options)
	   */
	@Override
	@SuppressWarnings("static-access")
	protected Options buildGeneralOptions(Options options) {
		options = super.buildGeneralOptions(options);
		options.addOption( OptionBuilder.withLongOpt("noDefaultDatasets")
						   .withDescription("Take the data sets paths from the command line instead of generating them." )
						   .create( "nDD" ) );
		options.addOption( OptionBuilder.withLongOpt("doDeleteInput")
				   .withDescription("If using the default datasets, and a file or directory exists with the name of the data set, delete it." )
				   .create( "dDI" ) );
		return options;
	}


	/** handle the --noDefaultDatasets and --doDeleteInput arguments.
	   * 
	   * @see com.apress.hadoopbook.utils.MainProgrameShell#processGeneralOptions(org.apache.hadoop.mapred.JobConf, org.apache.commons.cli.CommandLine)
	   * 
	   */
	  
	protected void processGeneralOptions(JobConf conf, CommandLine commandLine) {
		  super.processGeneralOptions( conf, commandLine );
		  if (commandLine.hasOption("nDD")) {
			  noDefaultDatasets = true;
			  if (verbose) { LOG.info("Not using default datasets."); }
		  }
		  if (commandLine.hasOption("dDI")) {
			  doDeleteInput = true;
			  if (verbose) { LOG.info("Delete of default input enabled."); }
		  }
	  }

	  /** Handle the custom arguments and flags.
	   * 
	   * If --noDefaultDatasets, at least 2 arguments must be left on the command line to use as data sets.
	   * 
	   * @see MainProgrameShell#handleRemainingArgs(JobConf conf, String[] args)
	   * 
	   */
	  protected boolean handleRemainingArgs(JobConf conf, String[] args) {
		  if (noDefaultDatasets&&args!=null&&args.length>0) {
			  LOG.error( "Unhandled arguments, " + args.length + " unhandled: "  + StringUtils.join( args, ", "));
			  return false;
		  }
		  if (noDefaultDatasets&&(args==null||args.length<2)) {
			  LOG.error( "the -nDD (--noDefaultDatasets) flag forces the requirement of at least 2 datasets as arguments." );
			  return false;
		  }

		  if (noDefaultDatasets) {
			  datasets.addAll(Arrays.<String>asList(args));
		  }
		  return true;
	}


	/** Given the resource names in {@link #defaultDatasetResourceNames} create those files in the current working directory and add them to the {@link #datasets} array.
	 * 
	 * The parameter {@link #doDeleteInput} is used to determine if an existing file of the name of the data set may be deleted
	 * @param conf The {@link JobConf} object to use for file access
	 * @throws IOException If an IO error happens or a data set already exists and {@link #doDeleteInput} is true.
	 */
	protected void generateDatasets(JobConf conf) throws IOException {
		for (String resource : defaultDatasetResourceNames) {
			InputStream is = null;
			FSDataOutputStream out = null;
			Path cwd = new Path(".");
			FileSystem fs = cwd.getFileSystem(conf);
			
			try {
				is = this.getClass().getResourceAsStream( "/" + resource );
				if (is==null) {
					throw new FileNotFoundException( "Unable to find default data set resource " + resource );
				}
				/** Get the trailing path name component of resource to use as the data set file name. */
				String[] parts = resource.split( "/" );
				/** The path to use for the data set. */
				Path destination = new Path( cwd, parts[parts.length-1] );
				
				/** Handle the deletion or exception case if the destination file already exists. */
				if (fs.exists(destination)) {
					if(!doDeleteInput) {
						throw new IOException( "Default input file " + destination + " exists and -dDI is not set.");
					} else if(!fs.delete(destination, true)) {
						throw new IOException( "Unable to delete " + destination);
					}
					if (verbose) { LOG.info( "Deleted existing default data set " + destination); }
				}
				/** Create the output file and copy the stream in. This version of copyBytes closes both the input and output on finish. */
				out = fs.create(destination, true);
				if (out==null) {
					throw new IOException( "Unable to create data set " + destination);
				}
				if (verbose) { LOG.info( "Copying resource /" + resource + " to " + destination); }
				IOUtils.copyBytes(is, out, conf);
				is=null;
				out=null;
				datasets.add(destination.toString());
			} finally {
				Utils.closeIf(is);
				Utils.closeIf(out);
			}
		}
	}
	
	/** Given a data set specification of the form path[:InputFormatClassName], return a map side join specifier for the data set.
	 * 
	 * The default InputFormat class is {@link KeyValueTextInputFormat} and the default package for class lookup is org.apache.hadoop.mapred.
	 * 
	 * @param conf the {@link JobConf} object to use for class lookups
	 * @param dataset The data set specifier
	 * @return The results of {@link CompositeInputFormat#compose(Class, String)}
	 * @throws IOException if the input format class can not be found.
	 */
	@SuppressWarnings("unchecked")
	public static String parseDataset( JobConf conf, String dataset ) throws IOException
	{
		String parts[] = dataset.split( ":" );
		/** Only accept strings that have 1 or to elements when split with a ':'. The strings have to not be 0 length. */
		if (parts.length<1 || parts.length>2 || parts[0].length()==0 || (parts.length>1 && parts[1].length()==0)) {
			throw new IOException( "Unable to parse " + dataset + " as a dataset[:input format] spec" );
		}
		/** The path to the data set. */
		final String path = parts[0];
		
		/** the class to use as the InputFormat for path. */
		Class<? extends InputFormat<?,?>> inputFormat;
		if (parts.length==1) {
			
			/** No {@link InputFormat} specifier, default to {@link KeyValueTextInputFormat}. */
			inputFormat = KeyValueTextInputFormat.class;
		} else {
			
			/** Work out the actual {@link InputFormat} class to use for the data set. */
			String classSpec = parts[1];
			if (classSpec.indexOf(".")==-1) {
				classSpec = "org.apache.hadoop.mapred." + classSpec;
			}
			inputFormat = (Class<? extends InputFormat<?, ?>>) conf.getClass( classSpec, null, InputFormat.class );
			if (inputFormat==null) {
				throw new IOException( "Unable to load a class that implements InputFormat for " + classSpec);
			}
		}

		/** Actually produce the specification string for this data set. */
		final String tableSpec = CompositeInputFormat.compose(inputFormat, path);
		return tableSpec;
	}

	/**	Given a join operator and a set of data set specifications (Path[:InputFormat]), return a full join command
	 * 
	 * This method will produce the join specifier, given a join operator and a set of inputs for the join.
	 * The assumption is that all inputs are individual data sets, and not joins.
	 * 
	 * An input set of the form op = inner, and datasets = a, b, c
	 * will return <code>inner(tbl(org.apache.mapred.KeyValueTextInputFormat.class,a),tbl(org.apache.mapred.KeyValueTextInputFormat.class,b),tbl(org.apache.mapred.KeyValueTextInputFormat.class,c))</code>
	 * which when stored in the configuration key <code>mapred.join.expr</code> will result in an inner join over a, b and c, with keys and values as Text.
	 * 
	 * @see CompositeInputFormat#compose(Class, String)
	 * 
	 * @param conf a {@link JobConf} object used for class loading of input format classes
	 * @param op	The join operator, one of inner, outer and override
	 * @param datasets	The list of data set specifiers. may not be empty
	 * @return The join string
	 * @throws IOException If if an input format specifier can not be mapped into a class implementing {@link InputFormat}.
	 */
	public static String compose( final JobConf conf, final String op, final List<String> datasets) throws IOException {
		if (datasets.isEmpty()) {
			throw new IOException("dataset for " + op + " may not be empty");
		}
		StringBuilder sb = new StringBuilder();
		sb.append(op);
		sb.append('(');
		/** Put all of the table specifiers in side of the () that attach to op. */
		for( String dataset : datasets ) {
			String spec = parseDataset(conf, dataset);
			sb.append(spec);
			sb.append(',');
		}
		sb.setLength(sb.length()-1);	/** Lose the trailing ','. */
		sb.append(")");
		return sb.toString();
	}
	
	/** This example runs three map/reduces, this method provides the common customization for the 3 map/reduces.
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#customSetup(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected
	void customSetup(JobConf baseConf) throws IOException {
		super.customSetup(baseConf);

		/** No data sets were passed on the command line, load the default set from the resources specified in {@link #defaultDatasetResourceNames}. */
		if (!noDefaultDatasets) {
			generateDatasets(baseConf);
		}
		/** All of the outputs are Text. */
		baseConf.setOutputFormat(TextOutputFormat.class);
		baseConf.setOutputKeyClass(Text.class);
		baseConf.setOutputValueClass(Text.class);
		
		/** setting the input format to {@link CompositeInputFormat} is the trigger for the map side join behavior. */
		baseConf.setInputFormat(CompositeInputFormat.class);
		/** Don't blow the memory on our small machine. */
		baseConf.setInt("io.sort.mb", 1);
		/** Only 1 reduce so all the data is in the same output file. */
		baseConf.setNumReduceTasks(0);
		/** Our specialty mapper and reducer. */
		baseConf.setMapperClass(DuplicateKeyIndicatingIdentityMapper.class);
		
		
	}
	
	
	
	/** This job runs 3 map reduce jobs in sequence and needs to handle it the run method directly.
	 * A join of inner, outer and override will be run over the input datasets.
	 * The output will be in MapSideJoinExample.inner, MapSideJoinExample.outer and MapSideJoinExample.override
	 * 
	 * @return 0 on complete success
	 * 
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#run(java.lang.String[])
	 */
	
	@Override
	public int run(String[] args) throws Exception {
		  final JobConf baseConf = new JobConf(getConf()); // Initialize the JobConf object to be used for this job from the command line configured JobConf.

		  /** Handle the base setup, if it fails we are out of here. This includes checking the remaining arguments for validity.
		   * args.length must be 0 if the default data sets are being used.
		   */
		  int ret = runSetup(args, baseConf);
		  if (ret!=0) {
			  return ret;
		  }
		  Path outputDirectoryBaseName = new Path("MapSideJoinExample");
		  
		  /** The number of jobs that have failed. */
		  int failCount=0;
		  /** The {@link RunningJob} objects will be saved in <code>jobs</code> and their counters dumped when all the jobs are complete. */
		  ArrayList<RunningJob> jobs = new ArrayList<RunningJob>();
		  
		  for (String join : joinTypes ) {
			  /** Produce the per job config object. */
				final JobConf conf = new JobConf(baseConf);
				conf.setJobName(join);
				final String joinStatement = compose(conf, join, datasets);
				if (verbose) {
					LOG.info( "The Join statement for " + join + " is " + joinStatement);
				}
				/** Just to be save replace any characters in <code>join</code> that would be bad in the file name portion of a path, and use that as the output directory suffix. */
				final String suffix = join.replaceAll("[/\\s]", "_");
				final Path outputPath = outputDirectoryBaseName.suffix("."+suffix);
				FileOutputFormat.setOutputPath(conf, outputPath);
				deleteOutputIf(conf);
				
				/** Store the join expression in the config so that the {@link CompositeInputFormat} can run the join. */
				conf.set("mapred.join.expr", joinStatement);
				
				if (join.equals("override")) {
					/** The override join does not pass TupleWritable objects to its {@link Mapper},
					 *  instead a single object of the type of the {@link InputFormat} value class is passed.
					 *  If all of the input tables do not have the same value class, it is unclear what the behavior is.
					 */
					conf.setMapOutputValueClass(Text.class);
					conf.setMapperClass(IdentityMapper.class);
					conf.setReducerClass(IdentityReducer.class);
				}
				try {
					/** Send the job to the framework. */
					RunningJob rj = launch(conf);
					jobs.add(rj);

					if (isSuccessFul(rj)!=0) {
						LOG.error( "error running join " + joinStatement);
						failCount++;
					}
				} catch( Throwable e) {
					LOG.error( "Job " + join + " failed ", e);
					failCount++;
				}
		  }
		  /** Emit the counters for the jobs. */
		  for( RunningJob rj : jobs) {
			  outputJobCounters(rj);
		  }
		  return failCount;
	  }

			
	/** If there were any exceptions the job did not succeed.
	 * 
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#isSuccessFul(org.apache.hadoop.mapred.RunningJob)
	 */
	protected int isSuccessFul(RunningJob rj) throws IOException
	{

		Counters counters = rj.getCounters();
	    long totalExceptions = 0;
		for( Counters.Group group : counters) {
	    	for( Counters.Counter counter: group) {
	    		if (counter.getDisplayName().equals("ReduceExceptionsTotal") ||
	    				counter.getDisplayName().equals("MapExceptionsTotal")) {
	    			totalExceptions  += counter.getCounter();
	    		}
	    	}
	    }
	    return (int) totalExceptions;
	}
		  


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapSideJoinExample(), args);
		if (res!=0) {
			System.err.println("Job exit code is " + res);
		}
		System.exit(res);

	}

}
