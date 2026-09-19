"""Keep Google discovery data selective; jobby.spec adds the two required docs."""

# The upstream PyInstaller hook copies every static Google API discovery
# document (about 100 MB). Package metadata and Jobby's Gmail/Calendar documents
# are already collected explicitly by jobby.spec.
datas = []
