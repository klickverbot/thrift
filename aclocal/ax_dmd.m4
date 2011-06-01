dnl @synopsis AX_DMD
dnl
dnl Test for the presence of a DMD-compatible D2 compiler, and (optionally)
dnl specified .
dnl
dnl If "DMD" is defined in the environment, that will be the only
dnl dmd command tested.  Otherwise, a hard-coded list will be used.
dnl
dnl After AX_DMD runs, the shell variables "success" and "ax_dmd" are set to
dnl "yes" or "no", and "DMD" is set to the appropriate commands.
dnl
dnl AX_CHECK_D_MODULE must be run after AX_DMD. It tests for the presence of a
dnl module in the import path of the chosen compiler, and sets the shell
dnl variable "success" to "yes" or "no".
dnl
dnl @category D
dnl @version 2011-05-31
dnl @license AllPermissive
dnl
dnl Copyright (C) 2009 David Reiss
dnl Copyright (C) 2011 David Nadlinger
dnl Copying and distribution of this file, with or without modification,
dnl are permitted in any medium without royalty provided the copyright
dnl notice and this notice are preserved.


AC_DEFUN([AX_DMD],
         [
          dnl Hard-coded default commands to test.
          DMD_PROGS="dmd,gdmd,ldmd"

          dnl Allow the user to specify an alternative.
          if test -n "$DMD" ; then
            DMD_PROGS="$DMD"
          fi

          AC_MSG_CHECKING(for DMD)

          echo "import std.algorithm; void main() {}" > configtest_ax_dmd.d
          success=no
          oIFS="$IFS"

          IFS=","
          for DMD in $DMD_PROGS ; do
            IFS="$oIFS"

            echo "Running \"$DMD configtest_ax_dmd.d\"" >&AS_MESSAGE_LOG_FD
            if $DMD configtest_ax_dmd.d >&AS_MESSAGE_LOG_FD 2>&1 ; then
              success=yes
              break
            fi
          done

          rm -f configtest_ax_dmd configtest_ax_dmd.o configtest_ax_dmd.d

          if test "$success" != "yes" ; then
            AC_MSG_RESULT(no)
            DMD=""
          else
            AC_MSG_RESULT(yes)
          fi

          ax_dmd="$success"
         ])


AC_DEFUN([AX_CHECK_D_MODULE],
         [
          AC_MSG_CHECKING(for D module [$1])

          echo "import $1; void main() {}" > configtest_ax_dmd.d

          echo "Running \"$DMD configtest_ax_dmd.d\"" >&AS_MESSAGE_LOG_FD
          if $DMD configtest_ax_dmd.d >&AS_MESSAGE_LOG_FD 2>&1 ; then
            AC_MSG_RESULT(yes)
            success=yes
          else
            AC_MSG_RESULT(no)
            success=no
          fi

          rm -f configtest_ax_dmd*
         ])
