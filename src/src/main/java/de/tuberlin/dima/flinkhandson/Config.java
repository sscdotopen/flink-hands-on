/**
 * Flink Hands-on
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.flinkhandson;

import org.apache.flink.shaded.com.google.common.io.Resources;

import java.io.File;

public class Config {
  /** Path to the directory receiving the output. */
  public final static String OUTPUT_PATH;
  /** Path to the directory containing the input files. */
  public final static String INPUT_PATH;

  static {
    File targetOutput = new File("target","output");
    if (targetOutput.exists()) {
      if (!targetOutput.isDirectory()) {
        throw new RuntimeException("Output directory is not initialized properly. Delete the 'output' directory in the root folder of this project. ");
      }
    } else {
      targetOutput.mkdir();
    }
    OUTPUT_PATH = targetOutput.getAbsolutePath();
    INPUT_PATH = new File("./src/main/resources").getAbsolutePath();
  }

  private Config() {
  }

  public static String inputPathTo(String file) {
    return INPUT_PATH+File.separator+file;
  }

  public static String outputPathTo(String file) {
    return OUTPUT_PATH+File.separator+file;
  }

  public static String outputPathToLogFile() {
    return new File(OUTPUT_PATH, "logfile").getAbsolutePath();
  }

}
