License:        BSD
Vendor:         Otus
Group:          PD01
URL:            http://otus.ru/lessons/3/
Source0:        otus-%{current_datetime}.tar.gz
BuildRoot:      %{_tmppath}/otus-%{current_datetime}
Name:           ip2w
Version:        0.0.1
Release:        1
BuildArch:      noarch
BuildRoot:      %{_tmppath}/%{name}-root
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
BuildRequires: systemd
Requires:	
Summary:  ...


%description
...
Git version: %{git_version} (branch: %{git_branch})

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot

%define __etcdir    /usr/local/etc
%define __logdir    /val/log/
%define __bindir    /usr/local/ip2w/
%define __systemddir	/usr/lib/systemd/system/

%prep
...

%install
[ "%{buildroot}" != "/" ] && rm -fr %{buildroot}
%{__mkdir} -p %{buildroot}/%{__systemddir}
%{__install} -pD -m 644 ... %{buildroot}/%{__systemddir}/%{name}.service
...

%post
%systemd_post %{name}.service
systemctl daemon-reload

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun %{name}.service

%clean
[ "%{buildroot}" != "/" ] && rm -fr %{buildroot}


%files
%{__logdir}
%{__bindir}
%{__systemddir}
%{__sysconfigdir}
...
