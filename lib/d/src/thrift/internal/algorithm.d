/**
 * Contains a self-contained copy of std.algorithm.remove to avoid
 * DMD @@BUG6395@@.
 */
module thrift.internal.algorithm;

import std.exception;
import std.functional;
import std.range;
import std.traits;

enum SwapStrategy
{
    /**
       Allows freely swapping of elements as long as the output
       satisfies the algorithm's requirements.
    */
    unstable,
    /**
       In algorithms partitioning ranges in two, preserve relative
       ordering of elements only to the left of the partition point.
    */
    semistable,
    /**
       Preserve the relative ordering of elements to the largest
       extent allowed by the algorithm's requirements.
    */
    stable,
}

Range remove(alias pred, SwapStrategy s = SwapStrategy.stable, Range)
(Range range)
if (isBidirectionalRange!Range)
{
    auto result = range;
    static if (s != SwapStrategy.stable)
    {
        for (;!range.empty;)
        {
            if (!unaryFun!(pred)(range.front))
            {
                range.popFront;
                continue;
            }
            move(range.back, range.front);
            range.popBack;
            result.popBack;
        }
    }
    else
    {
        auto tgt = range;
        for (; !range.empty; range.popFront)
        {
            if (unaryFun!(pred)(range.front))
            {
                // yank this guy
                result.popBack;
                continue;
            }
            // keep this guy
            move(range.front, tgt.front);
            tgt.popFront;
        }
    }
    return result;
}

void move(T)(ref T source, ref T target)
{
    if (&source == &target) return;
    assert(!pointsTo(source, source));
    static if (is(T == struct))
    {
        // Most complicated case. Destroy whatever target had in it
        // and bitblast source over it
        static if (hasElaborateDestructor!T) typeid(T).destroy(&target);
        memcpy(&target, &source, T.sizeof);
        // If the source defines a destructor or a postblit hook, we must obliterate the
        // object in order to avoid double freeing and undue aliasing
        static if (hasElaborateDestructor!T || hasElaborateCopyConstructor!T)
        {
            static T empty;
            memcpy(&source, &empty, T.sizeof);
        }
    }
    else
    {
        // Primitive data (including pointers and arrays) or class -
        // assignment works great
        target = source;
        // static if (is(typeof(source = null)))
        // {
        //     // Nullify the source to help the garbage collector
        //     source = null;
        // }
    }
}
