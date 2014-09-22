########################################
# Copyright (c) IBM Corp. 2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
#
# Contributors:
#    arayshu, lschneid - initial implementation
########################################
BASENAME=skvdoc


TEX_FILES=${BASENAME}.tex \
	client_api.tex \
    runtime.tex \
    back_end_api.tex \
	skv_macros.sty \



PDF_TARGET=${BASENAME}.pdf
TEXCMD=pdflatex



${PDF_TARGET}: ${TEX_FILES}
	${TEXCMD} ${BASENAME}
	@while (grep -c "Rerun " ${BASENAME}.log); do ${TEXCMD} ${BASENAME};  done


.PHONY: clean
clean:
	-rm -f ${PDF_TARGET} *.aux *.dvi *.log *.toc
