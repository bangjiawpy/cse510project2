package iterator;


import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
/**
 *
 *  This file contains an implementation of the nested loops skyline
 *  The algorithm is like:
 *
 *      foreach tuple r in outer_loop do
 *          foreach tuple s in inner_loop do
 *          	if s is dominated by r, delete s
 *          	if r is dominated by s, break and get a new tuple from outer loop
 *          if r is not dominated by all s, it is a skyline
 *          else it is not
 *
 */

public class NestedLoopsSky  extends Iterator
{
    private AttrType      _in1[],  _in2[];
    private   int        in1_len, in2_len;
    private   Iterator  outer;
    private   short t1_str_sizescopy[];
    private   CondExpr OutputFilter[];
    private   CondExpr RightFilter[];
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean        done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private   Tuple     outer_tuple, inner_tuple;
    private   Tuple     Jtuple;           // Joined tuple
    private   FldSpec   perm_mat[];
    private   int        nOutFlds;
    private   Heapfile  hf, hf_copy;
    private   Scan      inner, tmp;
    private   int       the_pref_list[];
    private   int       the_pref_list_length;


    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@param in1  Array containing field types of R.
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param am1  access method for outer i/p to join
     *@param relationName  access hfapfile for the data i/p to join
     *@param pref_list shows what attributes are preferred
     *@param pref_list_length number of attributes preferred
     *@param n_pages indicates the available pages for the operation
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */
    public NestedLoopsSky( AttrType    in1[],
                           int     len_in1,
                           short   t1_str_sizes[],
                           Iterator     am1,
                           String relationName,
                           int pref_list[],
                           int pref_list_length,
                           int n_pages
    ) throws IOException,NestedLoopException
    {

        _in1 = new AttrType[in1.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        in1_len = len_in1;


        outer = am1;
        t1_str_sizescopy =  t1_str_sizes;
        the_pref_list = pref_list;
        the_pref_list_length = pref_list_length;
        n_buf_pgs    = n_pages;
        inner = null;
        done  = false;
        inner_tuple = new Tuple();
        get_from_outer = true;
        short[]    t_size;

        try {
            hf = new Heapfile(relationName);

        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new heapfile failed.");
        }
        try {
            hf_copy = new Heapfile("hf_copy_nls");
            hf_copy.deleteFile();
            hf_copy = new Heapfile("hf_copy_nls");
        }
        catch(Exception e){
            throw new NestedLoopException(e, "Create new temporary heapfile failed.");
        }

        try {
            tmp = hf.openScan();
        }
        catch(Exception e){
            throw new NestedLoopException(e, "openScan failed");
        }
        RID rid_tmp = new RID();
        try{
            while((inner_tuple = tmp.getNext(rid_tmp)) != null){
                hf_copy.insertRecord(inner_tuple.returnTupleByteArray());
            }
        }
        catch(Exception e){
            throw new NestedLoopException(e, "temp heap file create failed");
        }
        tmp.closescan();
        tmp = null;



        //System.out.println("object create over");
    }

    /**
     *@return The joined tuple is returned
     *@exception IOException I/O errors
     *@exception JoinsException some join exception
     *@exception IndexException exception from super class
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception TupleUtilsException exception from using tuple utilities
     *@exception PredEvalException exception from PredEval class
     *@exception SortException sort exception
     *@exception LowMemException memory error
     *@exception UnknowAttrType attribute type unknown
     *@exception UnknownKeyTypeException key type unknown
     *@exception Exception other exceptions

     */
    public Tuple get_next()
            throws IOException,
            JoinsException ,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception
    {


        if (done)
            return null;

        do
        {

            if (get_from_outer == true)			// get one tuple from outer loop, refresh the inner loop
            {
                get_from_outer = false;
                outer_tuple=outer.get_next();
                if (outer_tuple== null)
                {
                    done = true;
                    return null;
                }


                if (inner != null)
                {	inner.closescan();
                    inner = null;}


                try {	hf_copy = null;
                    hf_copy = new Heapfile("hf_copy_nls");
                    inner = hf_copy.openScan();				// open the replicated data as inner loop
                }
                catch(Exception e){
                    throw new NestedLoopException(e, "openScan failed");
                }


            }  // ENDS: if (get_from_outer == TRUE)



            boolean skl = true;
            RID rid = new RID();
            while (true)      // get from inner
            {
                try{inner_tuple = inner.getNext(rid);}catch (Exception e){break;}
                if (inner_tuple ==null) break;
                inner_tuple.setHdr((short)in1_len, _in1,t1_str_sizescopy);
  //              System.out.println("outer and inner tuples");
    //            outer_tuple.print(_in1);
      //          inner_tuple.print(_in1);
//
                if (TupleUtils.Dominates(outer_tuple, _in1, inner_tuple, _in1, (short)in1_len,t1_str_sizescopy, the_pref_list, the_pref_list_length)
                        ==0)                // if outer tuple don't dominate inner tuple
                //if (true)
                {
                    //				System.out.println("outer cannot dominate inner");
//
                    if (TupleUtils.Dominates(inner_tuple, _in1, outer_tuple, _in1,(short)in1_len, t1_str_sizescopy, the_pref_list,
                            the_pref_list_length)==1){  // if inner tuple dominates outer tuple
                        //if (false){
//						System.out.println("inner dominate outer");
                        skl = false;
                        break;
                    }

                }
                else if (TupleUtils.Dominates(inner_tuple, _in1, outer_tuple, _in1,(short)in1_len, t1_str_sizescopy, the_pref_list,
                        the_pref_list_length)!=1) {
//					System.out.println("delete record");
                    //inner.closescan();
                    //hf_copy.deleteRecord(rid);
                    // discard the dominated data in inner loop (replicated file)

                }
            }

            if (skl == true){			// it is a skyline
                get_from_outer = true;  // get next outer tuple
//                inner.closescan();
//                inner = null;
                return outer_tuple;
            }
            else{
                get_from_outer = true; // get next outer tuple
            }


        } while (true);
    }

    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws JoinsException, IOException,IndexException
    {
        if (!closeFlag) {
            try {	outer.close();
                if(inner!=null){
                    inner.closescan();
                    inner = null;}

            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsSky.java: error in closing iterator.");
            }
//			if (hf_copy!=null){
//			try{hf_copy.deleteFile();}
//catch(Exception e){throw new JoinsException(e, "NestedLoopsSky.java: error in closing copy file.");}
//}


            closeFlag = true;
        }
    }
}

