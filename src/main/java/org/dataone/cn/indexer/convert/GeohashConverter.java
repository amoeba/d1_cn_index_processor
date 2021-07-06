/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.indexer.convert;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;

/**
 * Convert latitude, longitude coordinates of a rectangle to the geohash of the
 * center of the rectangle. The rectangle is specified as longmin, latmin,
 * longmax, latmax. If a single point is specified, then it will be latitude,
 * longitude
 */

public class GeohashConverter implements IConverter {

    // Default geohash length
    private int length = 9;

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    /**
     * @param latlong
     *            coords of a bounding box (latmax, latmin, longmax, longmin) or a single point (lat,
     *            long)
     * @return geohash string for the center of the bounding box or the
     *         specifice point
     */
    public String convert(String latlong) {

        String geohash = null;
        double geohashLat = 0;
        double geohashLong = 0;

        // Note we refer to the International Date Line as a hypothetical line that is located
        // at -180 west, from latitude 90N to -90S. This is just for convenience, to describe the
        // min and max values of this coordinate system, where bounding box center point calculations
        // need to take the limits of this coordinate system into account if the bounding box crosses them
        // The actual IDL is not continuous at -180W.
        String[] coords = latlong.split(" ");

        if (coords.length == 2) {
            geohashLat = Double.parseDouble(coords[0]);
            geohashLong = Double.parseDouble(coords[1]);
        } else if (coords.length == 4) {
            // Input string is west, south, east, north
            double northCoord = Double.parseDouble(coords[0]);
            double southCoord = Double.parseDouble(coords[1]);
            double eastCoord = Double.parseDouble(coords[2]);
            double westCoord = Double.parseDouble(coords[3]);

            // In some cases the the lat and long values for the bounding coords
            // can be equal, if it is intended to use four coords to specify a
            // single point, i.e. 
            //west = -119.1234 south=34.5678 east = -119.1234 north=34.5678.
            if (westCoord == eastCoord || southCoord == northCoord) {
                geohashLat = southCoord;
                geohashLong = westCoord;
            } else {
                // Calculate the bbox centerpoint - this lat, long will be used to calculate the
                // geohash.
                if (southCoord > northCoord)
                    throw new IllegalArgumentException("The southLatitude must not be greater than the northLatitude");

                if (Math.abs(southCoord) > 90 || Math.abs(northCoord) > 90 || Math.abs(westCoord) > 180 || Math.abs(eastCoord) > 180) {
                    throw new IllegalArgumentException("The supplied coordinates are out of range.");
                }

                // Does the bounding box cross the hypothetical IDL? The following will only be true if 
                // the bbox does cross the hypothetical IDL. If it does, then normalize coords to 0 to 360 for the 
                // calculation. Adding 360 to a negative longitude normalizes it to be the same spot on the earth 
                // but in a coord system with longitude ranging from 0 to 360. We can then use this normalized value 
                // to perform the center point calculation.
                if (eastCoord < westCoord) {
                    if(eastCoord < 0.0) eastCoord += 360.0;
                }

                double centerLatitude = (southCoord + northCoord) / 2.0;
                double centerLongitude = (westCoord + eastCoord) / 2.0;

                // convert back to -180 > coord > 180 if needed
                if (centerLongitude > 180.0) centerLongitude -= 360.0;

                geohashLat = centerLatitude;
                geohashLong = centerLongitude;
            }
        } else {
            return null;
        }

        try {
            geohash = GeoHash.withCharacterPrecision(geohashLat, geohashLong, length).toBase32();
        } catch (IllegalArgumentException iae) {
            return null;
        }
        
        //System.out.println("geohashLat, geohashLong, geohash: " + geohashLat + ", " + geohashLong + ", " + geohash);
        
        return geohash;
    }
}
