#!/bin/bash
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Program : sqoop mysql导入hive脚本                             #
# Version : Sqoop 1.4.6 MySQL 5.7.16                            #
# Author  : xxx                             #
# Date    : 2018-02-05                                          #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


function usage(){
        echo "$0 [ -t mysql_table_name ] | [ -a all ]"
}
if [ $# -ne 2 ];then
	usage;
	echo "Now you can pass only a single argument to an action."
	echo "{bash $0 -t t_bim_topic}  or  {bash $0 -a all}"
	exit;
fi

# 读取配置文件中的所有变量，设置为全局变量
# 配置文件
conf_file="/home/sqoop/mysql_hive/conf/mysql_hive.conf"
# MySQL数据库连接用户
db_user=`sed '/^db_user=/!d;s/.*=//' ${conf_file}`
# MySQL数据库连接密码
db_password=`sed '/^db_password=/!d;s/.*=//' ${conf_file}`
# MySQL数据库IP
db_host=`sed '/^db_host=/!d;s/.*=//' ${conf_file}`
# MySQL数据库端口
db_port=`sed '/^db_port=/!d;s/.*=//' ${conf_file}`
# MySQL导出数据库
db_name=`sed '/^db_name=/!d;s/.*=//' ${conf_file}`
# MySQL导出数据表文件
table_file=`sed '/^table_file=/!d;s/.*=//' ${conf_file}`
# sqoop工具路径
sqoop_dir=`sed '/^sqoop_dir=/!d;s/.*=//' ${conf_file}`
# 日志文件
sqoop_log_dir=`sed '/^sqoop_log_dir=/!d;s/.*=//' ${conf_file}`
# 判断hive中表是否存在的文件路径（0：不存在；1：存在）
hive_exists_dir=`sed '/^hive_exists_dir=/!d;s/.*=//' ${conf_file}`
# MySQL表前缀
mysql_prefix=`sed '/^mysql_prefix=/!d;s/.*=//' ${conf_file}`
# hive表前缀
hive_prefix=`sed '/^hive_prefix=/!d;s/.*=//' ${conf_file}`
# 输出目录生成的代码
out_dir=`sed '/^out_dir=/!d;s/.*=//' ${conf_file}`
# HDFS parent for table destination
warehouse_dir=`sed '/^warehouse_dir=/!d;s/.*=//' ${conf_file}`
# 获取邮件告警收信人
sqoop_receiver=`sed '/^sqoop_receiver=/!d;s/.*=//' ${conf_file}`

declare mysql_table
declare mysql_all

# sqoop日期
sqoop_date=`date +%F`
sqoop_yesterday=`date +%F -d -1day`
sqoop_time=`date +%H:%M:%S`
sqoop_week_day=`date +%u`
sqoop_dt=`date +%y%m%d_%H%M%S`
sqoop_dtt=`date +%y%m%d%H%M%S`

# 获取传递参数项
while getopts "t:a:" arg
do
	case ${arg} in
         t)
                mysql_table="${OPTARG}"
         ;;
         a)
                mysql_all="${OPTARG}"
         ;;
         ?)
                { usage; exit 1; }
         ;;
        esac
done
# 如果没有传递-t参数设置为0
if [ -z ${mysql_table} ]
then
        mysql_table_judge=0
else
	mysql_table_judge=1
fi
# 如果没有传递-a参数设置为0
if [[ -z ${mysql_all}  || ${mysql_all} != 'all' ]]
then
	mysql_all_judge=0
else
	mysql_all_judge=1
fi

# 通过sqoop列出MySQL数据库中所有的表
function list_table(){
	${sqoop_dir}/sqoop list-tables \
		--connect "jdbc:mysql://${db_host}:${db_port}/${db_name}?useSSL=false" \
		--username ${db_user} --password "${db_password}" > ${sqoop_log_dir}/${db_name}_list_table.log 2>&1
	return $?
}

# 判断表是否可以执行该脚本
function list_table_judge(){
	list_table
	sqoop_3k=$?
        if [ 0 -eq "${sqoop_3k}" ]; then
		while read line
		do
			if [[ ${mysql_tb} = ${line} ]]
			then
				table_exists_num=1
				break
			else
				table_exists_num=0
			fi
		done < ${sqoop_log_dir}/${db_name}_list_table.log
	else
		table_exists_num=0
	fi
	return ${table_exists_num}
}

# hive中表不存在，首次执行时创建[mysql_table_name].hive文件
function create_exists_hive_table(){
	hive_exists_file=${hive_exists_dir}/${mysql_tb}.hive
	if [ ! -f "${hive_exists_file}" ];
	then
        	echo "0" > ${hive_exists_dir}/${mysql_tb}.hive
 	fi
}

# 更新hive中表是否存在为1：存在
function update_exists_hive_table(){
	echo "1" > ${hive_exists_dir}/${mysql_tb}.hive
}

# 更新执行日期
function update_execution_date(){
	echo "${sqoop_yesterday}" > ${hive_exists_dir}/${mysql_tb}.date
}

# 根据执行日期文件判断是否可执行导入操作，避免重复导入报错
function judge_execution_date(){
	last_execution_date=`cat ${hive_exists_dir}/${mysql_tb}.date`
	t1=`date -d "${sqoop_yesterday}" +%s`
	t2=`date -d "${last_execution_date}" +%s`
	if [ ${t1} -le ${t2} ];then
		judge_date_num=0
	else
		judge_date_num=1
	fi
	return ${judge_date_num}
}

# 复制表结构
function create_hive_table(){
	echo "将MySQL的${db_name}.${mysql_tb}表结构复制到Hive的default库中" >> ${sqoop_log_dir}/${mysql_tb}.log
	${sqoop_dir}/sqoop create-hive-table  \
		--connect "jdbc:mysql://${db_host}:${db_port}/${db_name}?useSSL=false" \
		--username ${db_user} --password "${db_password}" \
		--table ${mysql_tb}  \
		--fields-terminated-by "\\001" \
		--hive-table ${hive_tb} >> ${sqoop_log_dir}/${mysql_tb}.log 2>&1
	return $?
}

# 首次追加数据
function import_first(){
	echo "将MySQL的${db_name}.${mysql_tb}表中${sqoop_yesterday} 23:59:59以前的数据导入hive中" >> ${sqoop_log_dir}/${mysql_tb}.log
	ctime_yesterday="${sqoop_yesterday} 23:59:59"
	${sqoop_dir}/sqoop import  \
		--connect "jdbc:mysql://${db_host}:${db_port}/${db_name}?useSSL=false" \
                --username ${db_user} --password "${db_password}" \
		--table ${mysql_tb}  \
		--where "ctime<='${ctime_yesterday}'" \
		--fields-terminated-by "\\001" \
		--outdir ${out_dir} \
		--warehouse-dir ${warehouse_dir}/${sqoop_dtt} \
		--hive-import \
		--hive-table ${hive_tb} >> ${sqoop_log_dir}/${mysql_tb}.log 2>&1
	return $?
}

# 追加数据
function import_after(){
        echo "将MySQL的${db_name}.${mysql_tb}表中[${sqoop_yesterday} 00:00:00,${sqoop_yesterday} 23:59:59]的数据导入hive中" >> ${sqoop_log_dir}/${mysql_tb}.log
        ctime_start="${sqoop_yesterday} 00:00:00"
	ctime_end="${sqoop_yesterday} 23:59:59"
        ${sqoop_dir}/sqoop import  \
                --connect "jdbc:mysql://${db_host}:${db_port}/${db_name}?useSSL=false" \
                --username ${db_user} --password "${db_password}" \
                --table ${mysql_tb}  \
                --where "ctime>='${ctime_start}' and ctime<='${ctime_end}'" \
                --fields-terminated-by "\\001" \
		--outdir ${out_dir} \
		--warehouse-dir ${warehouse_dir}/${sqoop_dtt} \
                --hive-import \
                --hive-table ${hive_tb} >> ${sqoop_log_dir}/${mysql_tb}.log 2>&1
        return $?
}

# 发生错误时立即发送邮件告警
function sqoop_error_to_email(){
	email_subject="${mysql_tb}数据导入hive状态"
	judge_num=`cat ${hive_exists_dir}/${mysql_tb}.hive`
	not_exists=0
	if test ${judge_num} -eq ${not_exists}
	then
		echo "首次追加数据，将MySQL的${db_name}.${mysql_tb}表中${sqoop_yesterday} 23:59:59以前的数据导入hive中失败！" | mutt ${sqoop_receiver} -s ${email_subject}
	else
		echo "${sqoop_yesterday}日追加数据，将MySQL的${db_name}.${mysql_tb}表中[${sqoop_yesterday} 00:00:00,${sqoop_yesterday} 23:59:59]的数据导入hive中失败！" | mutt ${sqoop_receiver} -s ${email_subject}	
	fi
}

# 通过sqoop从MySQL的数据导入到hive
function sqoop_mysql_hive(){
	# while循环获取需要从MySQL中导出数据的表
	while read line
	do
		# 获取MySQL表名和hive表名
		mysql_tb=${line}
		# 判断表是否可以执行导入操作
		list_table_judge
		table_exists_num=${table_exists_num}
		not_exists=0
		if test ${table_exists_num} -eq ${not_exists}
		then
			echo "###${sqoop_date} ${sqoop_time}###${mysql_tb}不能从MySQL导入到hive....." >> ${sqoop_log_dir}/error.log
			# 跳出当前循环
			continue
		fi
		hive_tb=`echo ${mysql_tb} | sed "s/^${mysql_prefix}/${hive_prefix}/"`
		# 若hive中表不存在，首次执行时创建[mysql_table_name].hive文件 
		create_exists_hive_table
		echo "##########${sqoop_date} ${sqoop_time}#########" >> ${sqoop_log_dir}/${mysql_tb}.log
		echo "mysql table:${mysql_tb} #--># hive table:${hive_tb}" >> ${sqoop_log_dir}/${mysql_tb}.log
		# 判断hive中该表是否存在
		judge_num=`cat ${hive_exists_dir}/${mysql_tb}.hive`
		not_exists=0
		if test ${judge_num} -eq ${not_exists}
		then
			# 复制表结构
			create_hive_table
			sqoop_0k=$?
			if [ 0 -eq "${sqoop_0k}" ]; then
				update_exists_hive_table
				echo "hive中${hive_tb}表已经存在，把${hive_exists_dir}/${mysql_tb}.hive更新为存在：1"  >> ${sqoop_log_dir}/${mysql_tb}.log
			else
				echo "将MySQL的${db_name}.${mysql_tb}表结构复制到Hive的default库中失败！"  >> ${sqoop_log_dir}/${mysql_tb}.log
				# 跳出当前循环
				continue
			fi
			# 首次追加数据
			import_first
			sqoop_1k=$?
			if [ 0 -eq "${sqoop_1k}" ]; then
				update_execution_date
                                echo "首次追加数据，将MySQL的${db_name}.${mysql_tb}表中${sqoop_yesterday} 23:59:59以前的数据导入hive中成功！"  >> ${sqoop_log_dir}/${mysql_tb}.log
                        else
                                echo "首次追加数据，将MySQL的${db_name}.${mysql_tb}表中${sqoop_yesterday} 23:59:59以前的数据导入hive中失败！"  >> ${sqoop_log_dir}/${mysql_tb}.log
				# 邮件告警
				sqoop_error_to_email
                                # 跳出当前循环
                                continue
                        fi
		else
			# 根据执行日期文件判断是否可执行导入操作，避免重复导入报错
			judge_execution_date
			judge_date_num=${judge_date_num}
			date_num=0
			if test ${judge_date_num} -eq ${date_num}
			then
				echo "${sqoop_yesterday}日的数据hive中已经存在，请勿重复操作....." >> ${sqoop_log_dir}/error.log
				# 跳出当前循环
				continue
			fi
			# 每日增量追加数据
			import_after
			sqoop_2k=$?
			if [ 0 -eq "${sqoop_2k}" ]; then
				update_execution_date
                                echo "${sqoop_yesterday}日追加数据，将MySQL的${db_name}.${mysql_tb}表中[${sqoop_yesterday} 00:00:00,${sqoop_yesterday} 23:59:59]的数据导入hive中成功！"  >> ${sqoop_log_dir}/${mysql_tb}.log
                        else
                                echo "${sqoop_yesterday}日追加数据，将MySQL的${db_name}.${mysql_tb}表中[${sqoop_yesterday} 00:00:00,${sqoop_yesterday} 23:59:59]的数据导入hive中失败！"  >> ${sqoop_log_dir}/${mysql_tb}.log
				# 邮件告警
				sqoop_error_to_email
                                # 跳出当前循环
                                continue
                        fi
		fi
	done < ${table_file}
}

# 通过sqoop从MySQL的指定数据表导入到hive
function sqoop_mysql_hive_only(){
	mysql_tb=${mysql_table}
	# 判断表是否可以执行导入操作
	list_table_judge
	table_exists_num=${table_exists_num}
	not_exists=0
	if test ${table_exists_num} -eq ${not_exists}
	then
		echo "###${sqoop_date} ${sqoop_time}###${mysql_tb}不能从MySQL导入到hive....."
		# 脚本执行结束
		exit
	fi
	hive_tb=`echo ${mysql_tb} | sed "s/^${mysql_prefix}/${hive_prefix}/"`
	# 若hive中表不存在，首次执行时创建[mysql_table_name].hive文件
        create_exists_hive_table
	echo "##########${sqoop_date} ${sqoop_time}#########" >> ${sqoop_log_dir}/${mysql_tb}.log
	echo "mysql table:${mysql_tb} #--># hive table:${hive_tb}" >> ${sqoop_log_dir}/${mysql_tb}.log
	# 判断hive中该表是否存在
	judge_num=`cat ${hive_exists_dir}/${mysql_tb}.hive`
	not_exists=0
	if test ${judge_num} -eq ${not_exists}
	then
		# 复制表结构
		create_hive_table
		sqoop_0k=$?
		if [ 0 -eq "${sqoop_0k}" ]; then
			update_exists_hive_table
			echo "hive中${hive_tb}表已经存在，把${hive_exists_dir}/${mysql_tb}.hive更新为存在：1"  >> ${sqoop_log_dir}/${mysql_tb}.log
		else
			echo "将MySQL的${db_name}.${mysql_tb}表结构复制到Hive的default库中失败！"  >> ${sqoop_log_dir}/${mysql_tb}.log
			exit
		fi
		# 首次追加数据
		import_first
		sqoop_1k=$?
		if [ 0 -eq "${sqoop_1k}" ]; then
			update_execution_date
			echo "首次追加数据，将MySQL的${db_name}.${mysql_tb}表中${sqoop_yesterday} 23:59:59以前的数据导入hive中成功！"  >> ${sqoop_log_dir}/${mysql_tb}.log
		else
			echo "首次追加数据，将MySQL的${db_name}.${mysql_tb}表中${sqoop_yesterday} 23:59:59以前的数据导入hive中失败！"  >> ${sqoop_log_dir}/${mysql_tb}.log
			# 邮件告警
			sqoop_error_to_email
			# 退出
			exit
		fi
	else
		# 指定表执行时不能增量追加数据
		# echo "指定表执行时不能增量追加数据！"  >> ${sqoop_log_dir}/${mysql_tb}.log
		# 根据执行日期文件判断是否可执行导入操作，避免重复导入报错
		judge_execution_date
		judge_date_num=${judge_date_num}
		date_num=0
		if test ${judge_date_num} -eq ${date_num}
		then
			echo "${sqoop_yesterday}日的数据hive中已经存在，请勿重复操作....."
			# 脚本执行结束
			exit
		fi
		# 增量追加昨天数据
		import_after
		sqoop_2k=$?
		if [ 0 -eq "${sqoop_2k}" ]; then
			update_execution_date
			echo "${sqoop_yesterday}日追加数据，将MySQL的${db_name}.${mysql_tb}表中[${sqoop_yesterday} 00:00:00,${sqoop_yesterday} 23:59:59]的数据导入hive中成功！"  >> ${sqoop_log_dir}/${mysql_tb}.log
		else
			echo "${sqoop_yesterday}日追加数据，将MySQL的${db_name}.${mysql_tb}表中[${sqoop_yesterday} 00:00:00,${sqoop_yesterday} 23:59:59]的数据导入hive中失败！"  >> ${sqoop_log_dir}/${mysql_tb}.log
			# 邮件告警
			sqoop_error_to_email
			# 退出
			exit
		fi
	fi
}

function main(){
	if [[ ${mysql_table_judge} -eq 0 && ${mysql_all_judge} -eq 1 ]]
	then
		sqoop_mysql_hive
	elif [[ ${mysql_table_judge} -eq 1 && ${mysql_all_judge} -eq 0 ]]
	then
		sqoop_mysql_hive_only
	else
		echo "parameter error occurs."
		usage
		exit	
	fi
}

main

