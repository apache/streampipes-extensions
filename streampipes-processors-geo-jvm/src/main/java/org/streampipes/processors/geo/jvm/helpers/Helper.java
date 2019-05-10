package org.streampipes.processors.geo.jvm.helpers;


import org.locationtech.jts.densify.Densifier;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

import java.util.ArrayList;
import java.util.Collection;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class Helper {





    /**
     * refines a LineString
     * @param geom input LineString geometry
     * @param distance distance for subpoints
     * @return LineString with added subpoints
     */
    public static LineString refineLine (LineString geom, Double distance){

        LineString internal = geom;

        if (!isMeterCRS(geom.getSRID())){
            internal = (LineString) transformSpGeom(internal, findWgsUtm_EPSG(extractPoint(internal)));
        }


        Densifier dens =  new Densifier(internal);
        dens.setDistanceTolerance(distance);
        LineString result = (LineString) dens.getResultGeometry();


        if ( internal.getSRID() != geom.getSRID()){
            result = (LineString) transformSpGeom(result, geom.getSRID());
        }

        return result;
    }





    /**
     * creates a voronoi polygon diagram
     * @param points MultiPoint
     * @return
     */
    public static MultiPolygon createVoronoi(MultiPoint points){


        MultiPoint internal = points;

        if (!isMeterCRS(points.getSRID())){
            internal = (MultiPoint) transformSpGeom(internal, findWgsUtm_EPSG(extractPoint(points)));
        }

        VoronoiDiagramBuilder voronoi = new VoronoiDiagramBuilder();
        voronoi.setSites(internal);

        Geometry plainResultVoronoi = voronoi.getDiagram(internal.getFactory());


        Collection<Geometry> collGeometry_voronoi = multiToSingle((GeometryCollection) plainResultVoronoi);
        // transform Collection of Geometry to Collection of Polygon
        Collection<Polygon> collPoly = new ArrayList<>();
        for (Geometry run1 : collGeometry_voronoi){
            collPoly.add((Polygon) createSPGeom(run1, run1.getSRID()));
        }

        MultiPolygon voronoi_result =  createSPMultiPolygon( collPoly, false);


        // return back ti input crs
        if (internal.getSRID() != points.getSRID()){
            voronoi_result = (MultiPolygon) transformSpGeom(voronoi_result, points.getSRID());
        }


        return voronoi_result;
    }







}
