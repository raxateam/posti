package com.itella.app.wbarchive;

import java.sql.*;
import java.util.*;

import com.workbrain.app.wbarchive.*;
import com.workbrain.app.wbarchive.core.ArchiveInteractions;
import com.workbrain.app.wbarchive.model.*;
/**
 * Overriding core class to allow purge of interactions
 */
public class ItellaArchiveInteractions extends ArchiveInteractions {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ItellaArchiveInteractions.class);


    /**
     * Purge allowed in Itella as workflow is not being utilized
     * @return
     */
    public boolean isTypePurgeImplemented() {
        return true;
    }

}
