Summary: SKV
Name: bgas-skv
Version: %{RPM_VERSION}
Release: 1
License: GPL
Group: BGAS
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
Complete SKV package

%package devel
Summary: SKV development package
Group: BGAS
%description devel
Header file(s) and libraries for the development of SKV clients.


%prep

%build

%install
rm -rf $RPM_BUILD_ROOT
make -C %{SKV_DIR} INSTALL_ROOTFS_DIR=$RPM_BUILD_ROOT SKV_CONF=rpm install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%exclude /opt/skv/include
%exclude /opt/skv/lib
/opt/skv
/etc/skv_server.conf

%files devel
%defattr(-,root,root,-)
/opt/skv/include
/opt/skv/lib

%doc

%changelog
* Fri Mar 11 2014 IBM
 - Initial build
