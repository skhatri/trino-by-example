
deploy_module(){
  local module_name=$1
  helm del ${module_name}|| true
  local values=${module_name}/values.yaml
  if [[ -f ${module_name}/local.yaml ]];
  then
    values="${values},${module_name}/local.yaml"
  fi;
  helm install ${module_name} ${module_name} -f ${values}
}

main=$1
if [[ ! -z ${main} ]];
then
  deploy_module ${main}
fi;

